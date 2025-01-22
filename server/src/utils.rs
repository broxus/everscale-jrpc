use crate::jrpc::JrpcError;
use crate::proto::ProtoError;
use bytes::Bytes;
use std::borrow::Cow;
use ton_block::MsgAddressInt;
use ton_types::UInt256;

macro_rules! define_query_error {
    ($ident:ident, { $($variant:ident => ($code:literal, $name:literal)),*$(,)? }) => {
        #[derive(Copy, Clone)]
        pub enum $ident {
            $($variant),*
        }

        impl $ident {
            fn info(&self) -> (i32, &'static str) {
                match self {
                    $(Self::$variant => ($code, $name)),*
                }
            }
        }
    };
}

define_query_error!(QueryError, {
    MethodNotFound => (-32601, "Method not found"),
    InvalidParams => (-32602, "Invalid params"),

    NotReady => (-32001, "Not ready"),
    NotSupported => (-32002, "Not supported"),
    ConnectionError => (-32003, "Connection error"),
    StorageError => (-32004, "Storage error"),
    FailedToSerialize => (-32005, "Failed to serialize"),
    InvalidAccountState => (-32006, "Invalid account state"),
    InvalidMessage => (-32007, "Invalid message"),
    TooBigRange => (-32008, "Too big range"),
});

impl QueryError {
    pub fn with_id(self, id: i64) -> JrpcError<'static> {
        let (code, message) = self.info();
        JrpcError::new(id, code, Cow::Borrowed(message))
    }

    pub fn without_id(self) -> ProtoError<'static> {
        let (code, message) = self.info();
        ProtoError::new(code, Cow::Borrowed(message))
    }
}

pub type QueryResult<T> = Result<T, QueryError>;

pub fn extract_address(address: &MsgAddressInt, target: &mut [u8]) -> QueryResult<()> {
    if let MsgAddressInt::AddrStd(address) = address {
        let account = address.address.get_bytestring_on_stack(0);
        let account = account.as_ref();

        if target.len() >= 33 && account.len() == 32 {
            target[0] = address.workchain_id as u8;
            target[1..33].copy_from_slice(account);
            return Ok(());
        }
    }

    Err(QueryError::InvalidParams)
}

pub fn hash_from_bytes(bytes: Bytes) -> Option<UInt256> {
    (bytes.len() == 32).then(|| UInt256::from_slice(&bytes))
}
