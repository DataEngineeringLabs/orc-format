mod boolean_rle;
mod rle_v2;

pub use boolean_rle::{BooleanIter, BooleanRleRunIter, BooleanRun};
pub use rle_v2::signed as v2_signed;
pub use rle_v2::unsigned as v2_unsigned;
pub use rle_v2::IteratorEnum;

pub fn deserialize_f32(stream: &[u8]) -> impl Iterator<Item = f32> + '_ {
    stream
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
}

pub fn deserialize_f64(stream: &[u8]) -> impl Iterator<Item = f64> + '_ {
    stream
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
}
