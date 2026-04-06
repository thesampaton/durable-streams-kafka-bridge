use crate::types::Offset;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub offset: Offset,
}

impl Default for SubscribeRequest {
    fn default() -> Self {
        Self {
            offset: Offset::Start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCheckpoint {
    pub next_offset: Offset,
    pub cursor: Option<String>,
    pub up_to_date: bool,
    pub stream_closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionEvent {
    Data(Bytes),
    Checkpoint(StreamCheckpoint),
}
