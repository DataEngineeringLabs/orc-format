//! Contains [`Decompressor`]
use std::io::Read;

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::Error;
use crate::proto::CompressionKind;

fn decode_header(bytes: &[u8]) -> (bool, usize) {
    let a: [u8; 3] = (&bytes[..3]).try_into().unwrap();
    let a = [0, a[0], a[1], a[2]];
    let length = u32::from_le_bytes(a);
    let is_original = a[1] & 1 == 1;
    let length = (length >> (8 + 1)) as usize;

    (is_original, length)
}

enum State<'a> {
    Original(&'a [u8]),
    Compressed(Vec<u8>),
}

struct DecompressorIter<'a> {
    stream: &'a [u8],
    current: Option<State<'a>>, // when we have compression but the value is original
    compression: CompressionKind,
    scratch: Vec<u8>,
}

impl<'a> DecompressorIter<'a> {
    pub fn new(stream: &'a [u8], compression: CompressionKind, scratch: Vec<u8>) -> Self {
        Self {
            stream,
            current: None,
            compression,
            scratch,
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        match self.current {
            Some(State::Compressed(some)) => some,
            _ => self.scratch,
        }
    }
}

impl<'a> FallibleStreamingIterator for DecompressorIter<'a> {
    type Item = [u8];

    type Error = Error;

    #[inline]
    fn advance(&mut self) -> Result<(), Self::Error> {
        if self.stream.is_empty() {
            self.current = None;
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
                    gz.read_to_end(&mut self.scratch)?;
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

/// A [`Read`]er fulfilling the ORC specification of reading compressed data.
pub struct Decompressor<'a> {
    decompressor: DecompressorIter<'a>,
    offset: usize,
    is_first: bool,
}

impl<'a> Decompressor<'a> {
    /// Creates a new [`Decompressor`] that will use `scratch` as a temporary region.
    pub fn new(stream: &'a [u8], compression: CompressionKind, scratch: Vec<u8>) -> Self {
        Self {
            decompressor: DecompressorIter::new(stream, compression, scratch),
            offset: 0,
            is_first: true,
        }
    }

    /// Returns the internal memory region, so it can be re-used
    pub fn into_inner(self) -> Vec<u8> {
        self.decompressor.into_inner()
    }
}

impl<'a> std::io::Read for Decompressor<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.is_first {
            self.is_first = false;
            self.decompressor.advance().unwrap();
        }
        let current = self.decompressor.get();
        let current = if let Some(current) = current {
            if current.len() == self.offset {
                self.decompressor.advance().unwrap();
                self.offset = 0;
                let current = self.decompressor.get();
                if let Some(current) = current {
                    current
                } else {
                    return Ok(0);
                }
            } else {
                &current[self.offset..]
            }
        } else {
            return Ok(0);
        };

        if current.len() >= buf.len() {
            buf.copy_from_slice(&current[..buf.len()]);
            self.offset += buf.len();
            Ok(buf.len())
        } else {
            buf[..current.len()].copy_from_slice(current);
            self.offset += current.len();
            Ok(current.len())
        }
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
