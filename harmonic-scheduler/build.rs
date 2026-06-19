fn main() {
    // Use the vendored protoc so the build does not depend on a system install,
    // matching how validator-protos builds.
    #[cfg(not(windows))]
    // SAFETY: build scripts run single-threaded before any user code.
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    tonic_prost_build::configure()
        .compile_protos(&["proto/backrun.proto"], &["proto"])
        .expect("compile backrun.proto");
}
