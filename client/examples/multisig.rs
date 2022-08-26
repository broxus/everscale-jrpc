use std::str::FromStr;
use std::time::Duration;

use ed25519_dalek::Signer;
use everscale_jrpc_client::{JrpcClientOptions, SendOptions, TransportErrorAction};
use nekoton::core::models::Expiration;
use nekoton::core::ton_wallet::multisig::prepare_transfer;
use nekoton::core::ton_wallet::{Gift, MultisigType, TransferAction, WalletType};
use nekoton::crypto::MnemonicType;
use ton_block::MsgAddressInt;

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter(Some("everscale_jrpc_client"), log::LevelFilter::Debug)
        .init();
    let seed = std::env::args().nth(1).unwrap();
    let to = std::env::args().nth(2).unwrap();

    let to = MsgAddressInt::from_str(&to).expect("invalid address");

    let client = everscale_jrpc_client::JrpcClient::new(
        ["https://jrpc.everwallet.net/rpc".parse().unwrap()],
        JrpcClientOptions {
            probe_interval: Duration::from_secs(10),
        },
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

    let tx = prepare_transfer(
        &nekoton::utils::SimpleClock,
        &signer.public,
        false,
        from,
        Gift {
            flags: 3,
            bounce: false,
            destination: to,
            amount: 1_000_000_000,
            /// can be built with `nekoton_abi::MessageBuilder`
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

    let signature = signer.sign(message.hash()).to_bytes();
    let signed_message = message.sign(&signature).expect("invalid signature");

    let message = signed_message.message;
    let send_options = SendOptions {
        error_action: TransportErrorAction::Return,
        ttl: Duration::from_secs(60),
        poll_interval: Duration::from_secs(1),
    };

    let status = client
        .send_message(message, send_options)
        .await
        .expect("failed to send message");
    println!("status: {:?}", status);
}

use nekoton::abi::{FunctionBuilder, MessageBuilder};

#[tokio::test]
pub async fn test_limit_order_back_swap() {
    use std::borrow::Cow;
    use std::str::FromStr;
    use std::time::Duration;

    use ed25519_dalek::ed25519::signature::Signer;
    use env_logger::Builder;
    use everscale_jrpc_client::{JrpcClientOptions, SendOptions, TransportErrorAction};
    use log::LevelFilter;
    use nekoton::core::models::Expiration;
    use nekoton::core::ton_wallet::multisig::prepare_transfer;
    use nekoton::core::ton_wallet::{Gift, MultisigType, TransferAction, WalletType};
    use nekoton::crypto::{MnemonicType, UnsignedMessage};
    use ton_block::MsgAddressInt;
    use url::Url;

    use crate::settings::config::Config;

    let mut builder = Builder::new();
    builder.filter_level(LevelFilter::Debug).init();

    let config = Config::new("Settings.toml");

    let seed = config.seed;

    let account_addr = "0:74be2f6664e2c84925ec9350f687c6f2eca642278ff3dc305dc29d023b73bafe";
    let to = MsgAddressInt::from_str(account_addr).unwrap();

    let endpoints: Result<Vec<_>, _> = (&config.states_rpc_endpoints)
        .iter()
        .map(|x| Url::parse(x.as_ref()).and_then(|x| x.join("/rpc")))
        .collect();

    let client = everscale_jrpc_client::JrpcClient::new(
        endpoints.expect("Bad endpoints"),
        JrpcClientOptions {
            probe_interval: Duration::from_secs(20),
        },
    )
    .await
    .unwrap();

    let signer =
        nekoton::crypto::derive_from_phrase(&seed, MnemonicType::Labs(0)).expect("invalid seed");

    let message = ton_block::Message::with_ext_in_header(ton_block::ExternalInboundMessageHeader {
        dst: to,
        ..Default::default()
    });

    let unsigned_message = nekoton::core::utils::make_labs_unsigned_message(
        &nekoton::utils::SimpleClock,
        message,
        Expiration::Timeout(360),
        &signer.public,
        Cow::Borrowed(create_message_function()),
        Vec::new(),
    )
    .unwrap();

    let signature = signer.sign(unsigned_message.hash()).to_bytes();
    let signed_message = unsigned_message
        .sign(&signature)
        .expect("invalid signature");

    let message = signed_message.message;
    let send_options = SendOptions {
        error_action: TransportErrorAction::Return,
        ttl: Duration::from_secs(360),
        poll_interval: Duration::from_secs(10),
    };

    let status = client
        .send_message(message, send_options)
        .await
        .expect("failed to send message");

    println!("status: {:?}", status);
}

macro_rules! once {
    ($ty:path, || $expr:expr) => {{
        static ONCE: once_cell::race::OnceBox<$ty> = once_cell::race::OnceBox::new();
        ONCE.get_or_init(|| Box::new($expr))
    }};
}

fn create_message_function() -> &'static ton_abi::Function {
    once!(ton_abi::Function, || {
        FunctionBuilder::new("backLimitOrderSwap")
            .abi_version(ton_abi::contract::ABI_VERSION_2_2)
            .build()
    })
}
