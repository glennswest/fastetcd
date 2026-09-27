//! TLS for the client port and the raft peer port (fastetcd#23).
//!
//! The two are configured independently, as in etcd: the client port
//! from `--cert-file`/`--key-file`/`--trusted-ca-file`/
//! `--client-cert-auth`, the peer port from the `--peer-*` flags. A
//! certificate trusted by the client port is therefore not trusted by
//! the peer port unless the peer CA also signed it, so the raft port can
//! require a different, usually narrower, authority.
//!
//! With peer TLS on, a member dials the others over TLS as well,
//! verifying their certificates against `--peer-trusted-ca-file` and
//! presenting its own peer certificate as its client certificate, so
//! `--peer-client-cert-auth` on every member gives mutual TLS between
//! members.

use std::path::{Path, PathBuf};

use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// One port's TLS files, and the flag names to use in errors about them.
#[derive(Debug, Clone, Default)]
pub struct TlsFiles {
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub trusted_ca_file: Option<PathBuf>,
    pub client_cert_auth: bool,
}

/// Which port a [`TlsFiles`] is for; names the flags in error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Port {
    Client,
    Peer,
}

impl Port {
    fn flag(self, name: &str) -> String {
        match self {
            Port::Client => format!("--{name}"),
            Port::Peer => format!("--peer-{name}"),
        }
    }
}

fn read(path: &Path, flag: &str) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| anyhow::anyhow!("{flag} {}: {e}", path.display()))
}

impl TlsFiles {
    /// Whether this port serves TLS at all.
    pub fn enabled(&self) -> bool {
        self.cert_file.is_some() || self.key_file.is_some()
    }

    fn identity(&self, port: Port) -> anyhow::Result<Option<Identity>> {
        let (cert_flag, key_flag) = (port.flag("cert-file"), port.flag("key-file"));
        match (&self.cert_file, &self.key_file) {
            (Some(c), Some(k)) => Ok(Some(Identity::from_pem(
                read(c, &cert_flag)?,
                read(k, &key_flag)?,
            ))),
            (None, None) => Ok(None),
            _ => anyhow::bail!("{cert_flag} and {key_flag} must both be set or both unset"),
        }
    }

    fn ca(&self, port: Port) -> anyhow::Result<Option<Certificate>> {
        let flag = port.flag("trusted-ca-file");
        self.trusted_ca_file
            .as_ref()
            .map(|p| read(p, &flag).map(Certificate::from_pem))
            .transpose()
    }

    /// Check the flags hang together, without reading any file.
    pub fn validate(&self, port: Port) -> anyhow::Result<()> {
        if self.cert_file.is_some() != self.key_file.is_some() {
            anyhow::bail!(
                "{} and {} must both be set or both unset",
                port.flag("cert-file"),
                port.flag("key-file")
            );
        }
        if self.client_cert_auth && self.trusted_ca_file.is_none() {
            anyhow::bail!(
                "{} requires {}",
                port.flag("client-cert-auth"),
                port.flag("trusted-ca-file")
            );
        }
        if self.client_cert_auth && !self.enabled() {
            anyhow::bail!(
                "{} requires {} and {}",
                port.flag("client-cert-auth"),
                port.flag("cert-file"),
                port.flag("key-file")
            );
        }
        if port == Port::Peer && self.enabled() && self.trusted_ca_file.is_none() {
            // Members dial each other too, and verify the certificate
            // they are shown against this CA. There is no system trust
            // store to fall back on, and a raft peer should never be
            // trusted on a public CA's word anyway.
            anyhow::bail!(
                "--peer-cert-file requires --peer-trusted-ca-file: members verify each \
                 other's peer certificates against it"
            );
        }
        Ok(())
    }

    /// The server side of this port, or `None` for plaintext.
    pub fn server_config(&self, port: Port) -> anyhow::Result<Option<ServerTlsConfig>> {
        self.validate(port)?;
        let Some(identity) = self.identity(port)? else {
            return Ok(None);
        };
        let mut cfg = ServerTlsConfig::new().identity(identity);
        if self.client_cert_auth {
            let ca = self.ca(port)?.expect("validated above");
            cfg = cfg
                .client_ca_root(ca)
                // Mandatory, not optional: a caller that presents no
                // certificate must fail the handshake. tonic 0.12
                // already defaults `client_auth_optional` to false, but
                // set it explicitly so the guarantee doesn't silently
                // depend on that default.
                .client_auth_optional(false);
        }
        Ok(Some(cfg))
    }

    /// How this member dials the other members' peer ports, or `None`
    /// for plaintext. Only meaningful for [`Port::Peer`].
    pub fn peer_client_config(&self) -> anyhow::Result<Option<ClientTlsConfig>> {
        self.validate(Port::Peer)?;
        let Some(identity) = self.identity(Port::Peer)? else {
            return Ok(None);
        };
        let ca = self.ca(Port::Peer)?.expect("validated above");
        Ok(Some(ClientTlsConfig::new().ca_certificate(ca).identity(identity)))
    }
}

/// Every peer URL this member listens on, advertises or dials must
/// agree with whether peer TLS is on: `https://` with it, `http://`
/// without. A URL with no scheme (`0.0.0.0:2380`, accepted for a listen
/// address) says nothing and passes. `what` names the flag for errors.
pub fn check_peer_url_schemes<'a>(
    peer_tls: bool,
    urls: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> anyhow::Result<()> {
    for (what, url) in urls {
        let url = url.trim();
        if peer_tls && url.starts_with("http://") {
            anyhow::bail!(
                "peer TLS is on (--peer-cert-file) but {what} has the plaintext URL {url}; \
                 use https://"
            );
        }
        if !peer_tls && url.starts_with("https://") {
            anyhow::bail!(
                "{what} has the https:// URL {url} but peer TLS is off: set \
                 --peer-cert-file, --peer-key-file and --peer-trusted-ca-file"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(cert: bool, key: bool, ca: bool, auth: bool) -> TlsFiles {
        TlsFiles {
            cert_file: cert.then(|| "c.pem".into()),
            key_file: key.then(|| "k.pem".into()),
            trusted_ca_file: ca.then(|| "ca.pem".into()),
            client_cert_auth: auth,
        }
    }

    fn err(f: &TlsFiles, port: Port) -> String {
        f.validate(port).unwrap_err().to_string()
    }

    #[test]
    fn plaintext_is_valid_on_both_ports() {
        files(false, false, false, false).validate(Port::Client).unwrap();
        files(false, false, false, false).validate(Port::Peer).unwrap();
    }

    #[test]
    fn cert_and_key_come_together_and_errors_name_the_port() {
        assert!(err(&files(true, false, true, false), Port::Peer)
            .contains("--peer-cert-file and --peer-key-file"));
        assert!(err(&files(false, true, false, false), Port::Client)
            .contains("--cert-file and --key-file"));
    }

    #[test]
    fn client_cert_auth_needs_a_ca_and_a_cert() {
        assert!(err(&files(true, true, false, true), Port::Peer)
            .contains("--peer-client-cert-auth requires --peer-trusted-ca-file"));
        assert!(err(&files(false, false, true, true), Port::Client)
            .contains("--client-cert-auth requires --cert-file"));
    }

    #[test]
    fn peer_tls_needs_the_peer_ca_but_client_tls_does_not() {
        assert!(err(&files(true, true, false, false), Port::Peer)
            .contains("--peer-cert-file requires --peer-trusted-ca-file"));
        files(true, true, false, false).validate(Port::Client).unwrap();
    }

    #[test]
    fn peer_url_schemes_must_match_peer_tls() {
        check_peer_url_schemes(false, [("--listen-peer-urls", "http://0.0.0.0:2380")]).unwrap();
        check_peer_url_schemes(true, [("--listen-peer-urls", "https://0.0.0.0:2380")]).unwrap();
        check_peer_url_schemes(true, [("--listen-peer-urls", "0.0.0.0:2380")]).unwrap();
        let e = check_peer_url_schemes(true, [("--initial-cluster", "http://h:2380")])
            .unwrap_err()
            .to_string();
        assert!(e.contains("--initial-cluster") && e.contains("http://h:2380"), "{e}");
        let e = check_peer_url_schemes(false, [("--listen-peer-urls", "https://h:2380")])
            .unwrap_err()
            .to_string();
        assert!(e.contains("peer TLS is off"), "{e}");
    }
}
