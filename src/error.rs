//! Contains [`Error`]
use crate::proto::stream::Kind;

/// Possible errors from this crate.
#[derive(Debug, Clone)]
pub enum Error {
    /// Generic error returned when the file is out of spec
    OutOfSpec,
    /// When a string column contains a value with invalid UTF8
    InvalidUtf8,
    /// When the user requests a column that does not exist
    InvalidColumn(u32),
    /// When the user requests a type that does not exist for the given column
    InvalidKind(u32, Kind),
    /// When decoding a float fails
    DecodeFloat,
    /// When decompression fails
    Decompression,
    /// When decoding the proto files fail
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
