use std::borrow::Cow;

use crate::jrpc::JrpcError;

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
}

pub type QueryResult<T> = Result<T, QueryError>;
