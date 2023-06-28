fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=src/network_protocol/network.proto");
    protobuf_codegen::Codegen::new()
        .pure()
        .include("src/")
        .input("src/rpc.proto")
        .cargo_out_dir("proto")
        .run()
}
