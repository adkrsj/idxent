use std::{env, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let output_dir = manifest_dir.join("src/findent/generated");
    println!("output_dir: {}", output_dir.display());
    let _ = std::fs::create_dir_all(output_dir.clone());

    println!("cargo:rerun-if-changed=build.rs");
    tonic_prost_build::configure()
        .out_dir(&output_dir)
        .compile_protos(&["proto/findent.proto"], &["proto"])
        .unwrap();

    println!("cargo:rerun-if-env-changed=GRPC_RUST_REGENERATE_PROTO");

    println!("cargo:rerun-if-changed=findent.proto");
    grpc_protobuf_build::CodeGen::new()
        .output_dir(output_dir)
        .input("findent.proto")
        .include(manifest_dir.join("proto"))
        .client_only()
        .compile()
        .unwrap();
}

