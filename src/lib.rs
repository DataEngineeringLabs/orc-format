use proto::stream::Kind;

pub mod proto;
pub mod read;

pub use fallible_streaming_iterator;

#[derive(Debug, Clone)]
pub enum Error {
    OutOfSpec,
    RleLiteralTooLarge,
    InvalidUtf8,
    InvalidColumn(u32, Kind),
}

impl From<prost::DecodeError> for Error {
    fn from(_: prost::DecodeError) -> Self {
        Self::OutOfSpec
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::OutOfSpec
    }
}
