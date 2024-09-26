use everscale_rpc_client::ClientOptions;

#[tokio::main]
async fn main() {
    let codehash = std::env::args().nth(1).expect("codehash is required");
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(tracing::Level::INFO)
        .init();

    let client = everscale_rpc_client::RpcClient::new(
        ["https://jrpc.everwallet.net/rpc".parse().unwrap()],
        ClientOptions::default(),
    )
    .await
    .unwrap();

    let code_hash = hex::decode(codehash).unwrap().try_into().unwrap();
    let count = count_accounts(&client, code_hash).await.unwrap();
    println!("Accounts count: {}", count);
}

async fn count_accounts(
    client: &everscale_rpc_client::RpcClient,
    code_hash: [u8; 32],
) -> anyhow::Result<u32> {
    let mut continuation = None;
    let mut count = 0;
    loop {
        let addrs = client
            .get_accounts_by_code_hash(code_hash, continuation.as_ref(), 100)
            .await?;

        continuation = addrs.last().cloned();
        count += addrs.len() as u32;
        if addrs.len() != 100 {
            break;
        }
    }
    Ok(count)
}
