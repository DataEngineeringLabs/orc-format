use crate::proto::stream::Kind;

#[derive(Debug, Clone)]
pub enum Error {
    OutOfSpec,
    RleLiteralTooLarge,
    InvalidUtf8,
    InvalidColumn(u32),
    InvalidKind(u32, Kind),
    DecodeFloat,
    Decompression,
    InvalidProto,
}

impl From<prost::DecodeError> for Error {
    fn from(_: prost::DecodeError) -> Self {
        Self::InvalidProto
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::OutOfSpec
    }
}
