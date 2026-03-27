use tonic_build::configure;

fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        unsafe {
            std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc())
        }
    }

    let proto_base_path = std::path::PathBuf::from("protos");

    let protos: Vec<_> = std::fs::read_dir(&proto_base_path)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "proto"))
        .inspect(|p| println!("cargo:rerun-if-changed={}", p.display()))
        .collect();

    configure()
        .build_client(true)
        .build_server(false)
        .compile(&protos, &[proto_base_path])
}
