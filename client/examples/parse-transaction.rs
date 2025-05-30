use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let client = everscale_rpc_client::RpcClient::new(
        ["https://jrpc.everwallet.net/rpc".parse().unwrap()],
        everscale_rpc_client::ClientOptions::default(),
    )
    .await
    .unwrap();

    let tx_hash = std::env::args().nth(1).expect("No arguments passed");
    let tx_hash = hex::decode(tx_hash).unwrap();
    anyhow::ensure!(tx_hash.len() == 32, "Invalid tx hash length");
    let tx_hash = ton_types::UInt256::from_slice(&tx_hash);

    let tx = client
        .get_raw_transaction(tx_hash)
        .await?
        .expect("Transaction not found");

    let abi = get_abi_for_account(&format!(
        "0:{}",
        hex::encode(tx.account_addr.get_bytestring(0))
    ))
    .await?;

    let parser = nekoton_abi::transaction_parser::TransactionParserBuilder::default()
        .function_in_list(abi.functions.values().cloned(), false)
        .functions_out_list(abi.functions.values().cloned(), false)
        .events_list(abi.events.values().cloned())
        .build()
        .unwrap();
    let parsed = parser.parse(&tx)?;

    for parsed in parsed {
        println!("{}: {:#?}", parsed.name, parsed.tokens);
    }

    Ok(())
}

async fn get_abi_for_account(contract_addr: &str) -> Result<ton_abi::Contract> {
    let abi = reqwest::get(format!(
        "https://verify.everscan.io/abi/address/{contract_addr}"
    ))
    .await?
    .text()
    .await?;

    let abi = ton_abi::Contract::load(abi.as_bytes())?;
    Ok(abi)
}
