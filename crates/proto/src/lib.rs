//! Generated etcd v3 gRPC stubs.
//!
//! Protos are vendored under `crates/proto/protos/etcd/api/` and generated
//! at build time by `build.rs` using `tonic-build`. See `vendor.sh` and
//! `strip_annotations.py` for the vendoring pipeline.

/// `etcdserverpb` — top-level KV / Watch / Lease / Cluster / Maintenance /
/// Auth services and their request/response messages.
pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

/// `mvccpb` — `KeyValue` and `Event` types shared by KV and Watch.
pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

/// `authpb` — `User`, `Role`, `Permission`, `UserAddOptions`.
pub mod authpb {
    tonic::include_proto!("authpb");
}
