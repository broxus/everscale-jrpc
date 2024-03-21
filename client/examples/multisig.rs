use std::io::Write;
use std::str::FromStr;
use std::time::Duration;

use ed25519_dalek::Signer;
use nekoton::core::models::Expiration;
use nekoton::core::ton_wallet::multisig::prepare_transfer;
use nekoton::core::ton_wallet::{Gift, MultisigType, TransferAction, WalletType};
use nekoton::crypto::{extend_with_signature_id, MnemonicType};
use reqwest::Client;
use serde_json::json;
use ton_block::{GetRepresentationHash, MsgAddressInt, Serializable};

use everscale_rpc_client::{
    ClientOptions, RpcClient, SendOptions, SendStatus, TransportErrorAction,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "everscale_rpc_client=info",
        ))
        .init();

    let mut times = std::fs::File::create("times.txt").unwrap();
    let seed = std::env::args().nth(1).unwrap();
    let to = std::env::args().nth(2).unwrap();

    let to = MsgAddressInt::from_str(&to).expect("invalid address");

    let client = everscale_rpc_client::RpcClient::new(
        ["https://jrpc.venom.foundation/rpc".parse().unwrap()],
        ClientOptions::default(),
    )
    .await
    .unwrap();

    let signer =
        nekoton::crypto::derive_from_phrase(&seed, MnemonicType::Labs(0)).expect("invalid seed");
    let from = nekoton::core::ton_wallet::compute_address(
        &signer.public,
        WalletType::Multisig(MultisigType::SafeMultisigWallet),
        0,
    );
    let mut succesful = 0;
    let mut failed = 0;

    let num_send = 30;

    for i in 0..num_send {
        let start = std::time::Instant::now();
        let tx = prepare_transfer(
            &nekoton::utils::SimpleClock,
            MultisigType::SafeMultisigWallet,
            &signer.public,
            false,
            from.clone(),
            Gift {
                flags: 3,
                bounce: false,
                destination: to.clone(),
                amount: 1_000_000,
                // can be built with `nekoton_abi::MessageBuilder`
                body: None,
                state_init: None,
            },
            Expiration::Timeout(60),
        )
        .unwrap();

        let message = match tx {
            TransferAction::DeployFirst => {
                panic!("DeployFirst not supported")
            }
            TransferAction::Sign(m) => m,
        };

        let signature = signer
            .sign(&*extend_with_signature_id(message.hash(), Some(1)))
            .to_bytes();
        let signed_message = message.sign(&signature).expect("invalid signature");

        let message = signed_message.message;
        let send_options = SendOptions {
            error_action: TransportErrorAction::Return,
            ttl: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
        };

        let cell = message
            .write_to_new_cell()
            .and_then(ton_types::BuilderData::into_cell)
            .unwrap();
        let boc = base64::encode(ton_types::serialize_toc(&cell).unwrap());
        let id = base64::encode(cell.repr_hash());
        let status = send_gql(boc, id, *cell.repr_hash().as_slice(), &client)
            .await
            .unwrap();
        // let status = client
        //     .send_message(message, send_options)
        //     .await
        //     .expect("failed to send message");
        match status {
            SendStatus::Confirmed(tx) => {
                succesful += 1;
                println!("tx: {}", tx.hash().unwrap().to_hex_string());
            }
            SendStatus::Expired => {
                println!("Message expired");
                failed += 1;
            }
            _ => { /* this method doesn't return other statuses */ }
        }

        let elapsed = start.elapsed();
        // times.push(elapsed.as_millis());
        times
            .write_all(format!("{},{}\n", i, elapsed.as_millis()).as_bytes())
            .unwrap();
        println!("Send {i} of {num_send} messages",);
    }
    println!("Succesful: {}, failed: {}", succesful, failed);
}

#[derive(Debug, serde::Serialize)]
struct GraphQLQuery<'a> {
    query: &'a str,
    variables: serde_json::Value,
}

async fn send_gql(
    message: String,
    id: String,
    x: [u8; 32],
    rpc: &RpcClient,
) -> anyhow::Result<SendStatus> {
    let client = Client::new();
    let query = "mutation($id:String!,$boc:String!){postRequests(requests:[{id:$id,body:$boc}])}";

    let variables = json!({
        "id": id,
        "boc": message,
    });

    let gql_query = GraphQLQuery { query, variables };

    let res = client
        // .post("https://gql.venom.foundation/graphql") // Specify the URL
        .post("http://57.128.125.221:8080/graphql") // Specify the URL
        .json(&gql_query) // Pass the constructed query payload
        .send()
        .await?;

    let res_text = res.text().await?;
    println!("Response: {}", res_text);
    let polling_start = std::time::Instant::now();

    loop {
        let tx = rpc.get_dst_transaction(&x).await?;
        if tx.is_some() {
            return Ok(SendStatus::Confirmed(tx.unwrap()));
        }
        if polling_start.elapsed().as_secs() > 60 {
            return Ok(SendStatus::Expired);
        }
    }
}
