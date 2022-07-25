use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::proto::CompressionKind;

use super::super::Error;

fn decode_header(bytes: &[u8]) -> (bool, usize) {
    let a: [u8; 3] = (&bytes[..3]).try_into().unwrap();
    let a = [0, a[0], a[1], a[2]];
    let length = u32::from_le_bytes(a);
    let is_original = a[1] & 1 == 1;
    let length = (length >> (8 + 1)) as usize;

    (is_original, length)
}

fn decompress_zlib<'a>(
    maybe_compressed: &'a [u8],
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8], Error> {
    let (is_original, length) = decode_header(maybe_compressed);
    let maybe_compressed = maybe_compressed
        .get(3..3 + length)
        .ok_or(Error::OutOfSpec)?;
    if is_original {
        return Ok(maybe_compressed);
    }
    let mut gz = flate2::read::DeflateDecoder::new(maybe_compressed);
    gz.read_to_end(scratch).unwrap();
    Ok(scratch)
}

pub fn maybe_decompress<'a>(
    maybe_compressed: &'a [u8],
    compression: CompressionKind,
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8], Error> {
    Ok(match compression {
        CompressionKind::None => maybe_compressed,
        CompressionKind::Zlib => decompress_zlib(maybe_compressed, scratch)?,
        _ => todo!(),
    })
}

enum State<'a> {
    Original(&'a [u8]),
    Compressed(Vec<u8>),
}

pub struct Decompressor<'a> {
    stream: &'a [u8],
    current: Option<State<'a>>, // when we have compression but the value is original
    compression: CompressionKind,
    scratch: Vec<u8>,
}

impl<'a> Decompressor<'a> {
    pub fn new(stream: &'a [u8], compression: CompressionKind, scratch: Vec<u8>) -> Self {
        Self {
            stream,
            current: None,
            compression,
            scratch,
        }
    }
}

impl<'a> FallibleStreamingIterator for Decompressor<'a> {
    type Item = [u8];

    type Error = Error;

    #[inline]
    fn advance(&mut self) -> Result<(), Self::Error> {
        if self.stream.is_empty() {
            return Ok(());
        }
        match self.compression {
            CompressionKind::None => {
                // todo: take stratch from current State::Compressed for re-use
                self.current = Some(State::Original(self.stream));
                self.stream = &[];
            }
            CompressionKind::Zlib => {
                // todo: take stratch from current State::Compressed for re-use
                let (is_original, length) = decode_header(self.stream);
                self.stream = &self.stream[3..];
                let (maybe_compressed, remaining) = self.stream.split_at(length);
                self.stream = remaining;
                if is_original {
                    self.current = Some(State::Original(maybe_compressed));
                } else {
                    let mut gz = flate2::read::DeflateDecoder::new(maybe_compressed);
                    self.scratch.clear();
                    gz.read_to_end(&mut self.scratch).unwrap();
                    self.current = Some(State::Compressed(std::mem::take(&mut self.scratch)));
                }
            }
            _ => todo!(),
        };
        Ok(())
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref().map(|x| match x {
            State::Original(x) => *x,
            State::Compressed(x) => x.as_ref(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_uncompressed() {
        // 5 uncompressed = [0x0b, 0x00, 0x00] = [0b1011, 0, 0]
        let bytes = &[0b1011, 0, 0, 0];

        let (is_original, length) = decode_header(bytes);
        assert!(is_original);
        assert_eq!(length, 5);
    }

    #[test]
    fn decode_compressed() {
        // 100_000 compressed = [0x40, 0x0d, 0x03] = [0b01000000, 0b00001101, 0b00000011]
        let bytes = &[0b01000000, 0b00001101, 0b00000011, 0];

        let (is_original, length) = decode_header(bytes);
        assert!(!is_original);
        assert_eq!(length, 100_000);
    }
}
