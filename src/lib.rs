pub mod proto;
pub mod read;

#[derive(Debug, Clone)]
pub enum Error {
    OutOfSpec,
}
