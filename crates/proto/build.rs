// Generate tonic gRPC stubs for the vendored etcd v3 .proto files.
//
// Vendored protos live under `protos/etcd/api/`. They are stripped of
// annotations we don't need; see `vendor.sh` and `strip_annotations.py`.

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    let protos = [
        proto_root.join("etcd/api/mvccpb/kv.proto"),
        proto_root.join("etcd/api/authpb/auth.proto"),
        proto_root.join("etcd/api/etcdserverpb/rpc.proto"),
    ];

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=protos");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Box recursive Compare → CompareTarget cycle so prost is happy.
        // (Compare in rpc.proto references itself indirectly via Txn.)
        .compile_protos(&protos, &[proto_root])?;

    Ok(())
}
