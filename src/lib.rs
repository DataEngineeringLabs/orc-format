pub mod proto;
pub mod read;

#[derive(Debug, Clone)]
pub enum Error {
    OutOfSpec,
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
