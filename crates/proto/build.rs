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
        proto_root.join("fastetcd/raft.proto"),
        proto_root.join("fastetcd/admin.proto"),
    ];

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=protos");

    let descriptors = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("descriptors.bin");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(&descriptors)
        // Box recursive Compare → CompareTarget cycle so prost is happy.
        // (Compare in rpc.proto references itself indirectly via Txn.)
        .compile_protos(&protos, &[proto_root])?;

    // serde impls for the etcd messages, for the v3 JSON gateway (#28).
    // etcd's gateway marshals with protojson `UseProtoNames` and
    // `DiscardUnknown`: proto field names out (both spellings accepted
    // in), unknown input fields ignored.
    let set = std::fs::read(&descriptors)?;
    pbjson_build::Builder::new()
        .register_descriptors(&set)?
        .preserve_proto_field_names()
        .ignore_unknown_fields()
        .build(&[".etcdserverpb", ".mvccpb", ".authpb"])?;

    Ok(())
}
