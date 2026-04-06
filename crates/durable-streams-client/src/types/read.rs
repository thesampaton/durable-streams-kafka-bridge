use crate::types::Offset;
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    CatchUp,
    LongPoll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    pub offset: Offset,
    pub mode: ReadMode,
    pub cursor: Option<String>,
}

impl Default for ReadRequest {
    fn default() -> Self {
        Self {
            offset: Offset::Start,
            mode: ReadMode::CatchUp,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub bytes: Bytes,
    pub content_type: Option<String>,
    pub next_offset: Offset,
    pub up_to_date: bool,
    pub stream_closed: bool,
    pub cursor: Option<String>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    pub content_type: Option<String>,
    pub next_offset: Offset,
    pub stream_closed: bool,
    pub etag: Option<String>,
}
