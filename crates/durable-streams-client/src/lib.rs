#![warn(clippy::all)]

mod client;
mod error;
mod protocol;
mod retry;
mod stream;
mod transport;
mod types;

pub use client::{ClientConfig, DurableStreamsClient};
pub use error::{Error, ErrorKind, HttpError};
pub use retry::RetryPolicy;
pub use stream::DurableStream;
pub use types::{
    Offset, ReadMode, ReadRequest, ReadResponse, StreamCheckpoint, StreamMetadata, StreamPath,
    SubscribeRequest, SubscriptionEvent,
};
