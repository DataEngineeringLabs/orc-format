#![doc = include_str!("lib.md")]
#![forbid(unsafe_code)]
pub mod error;
pub mod proto;
pub mod read;

pub use fallible_streaming_iterator;
