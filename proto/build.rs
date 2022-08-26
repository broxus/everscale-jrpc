fn main() {
    let proto = "src/rpc.proto";

    let mut config = prost_build::Config::new();
    config.bytes(&["."]);

    tonic_build::configure()
        .build_server(cfg!(feature = "server"))
        .build_client(cfg!(feature = "client"))
        .compile_with_config(config, &[proto], &["src"])
        .unwrap();

    // prevent needing to rebuild if files (or deps) haven't changed
    println!("cargo:rerun-if-changed={}", proto);
}
