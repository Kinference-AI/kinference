fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The vendored `protoc`, so the build doesn't depend on a system
    // protobuf compiler.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    // SAFETY: build.rs runs single-threaded before any user code;
    // there are no other threads to race with on this env var.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
        std::env::set_var("PROTOC_INCLUDE", &protoc_include);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/kinference.proto"],
            &[
                "proto",
                protoc_include
                    .to_str()
                    .expect("protoc include path is utf-8"),
            ],
        )?;
    Ok(())
}
