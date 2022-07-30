//! Contains different iterators that receive a reader ([`std::io::Read`])
//! and return values for each of ORC's physical types (e.g. boolean).
mod boolean_rle;
mod float;
mod rle_v2;
mod variable_length;

pub use boolean_rle::{BooleanIter, BooleanRleRunIter, BooleanRun};
pub use float::{Float, FloatIter};
pub use rle_v2::{
    SignedRleV2Iter, SignedRleV2Run, SignedRleV2RunIter, UnsignedRleV2Iter, UnsignedRleV2Run,
    UnsignedRleV2RunIter,
};
pub use variable_length::Values;

#[inline]
fn read_u8<R: std::io::Read>(reader: &mut R) -> Result<u8, std::io::Error> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}
