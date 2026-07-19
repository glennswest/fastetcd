// The doc comments in the generated stubs are etcd's own, copied
// verbatim from the upstream .proto files; their list formatting trips
// this lint and isn't ours to reformat.
#![allow(clippy::doc_lazy_continuation)]

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

/// fastetcd-internal Raft peer RPC. Not part of the etcd v3 wire
/// protocol — used between fastetcd nodes for openraft messages.
pub mod fastetcd_raft {
    tonic::include_proto!("fastetcd.raft");
}
