pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

pub use prost;
