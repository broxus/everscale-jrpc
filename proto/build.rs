fn main() -> anyhow::Result<()> {
    prost_build::compile_protos(&["src/rpc.proto"], &["src/"]).unwrap();
    Ok(())
}
