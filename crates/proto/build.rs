// Placeholder build script. Once etcd .proto files are vendored under
// `crates/proto/protos/`, this script will invoke `tonic_build::configure()`
// to generate gRPC stubs.

fn main() {
    println!("cargo:rerun-if-changed=protos/");
    // Intentionally a no-op until protos are vendored — keeps the workspace
    // compiling on a fresh clone before proto vendoring is complete.
}
