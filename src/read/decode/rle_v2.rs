use std::io::Read;

use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EncodingTypeV2 {
    ShortRepeat,
    Direct,
    PatchedBase,
    Delta,
}

fn header_to_rle_v2_short_repeated_width(header: u8) -> u8 {
    (header & 0b00111000) >> 3
}

fn header_to_rle_v2_short_repeated_count(header: u8) -> u8 {
    header & 0b00000111
}

fn rle_v2_direct_bit_width(value: u8) -> u8 {
    match value {
        0 => 1,
        1 => 2,
        3 => 4,
        7 => 8,
        15 => 16,
        23 => 24,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        other => todo!("{other}"),
    }
}

fn header_to_rle_v2_direct_bit_width(header: u8) -> u8 {
    let bit_width = (header & 0b00111110) >> 1;
    rle_v2_direct_bit_width(bit_width)
}

fn rle_v2_delta_bit_width(value: u8) -> u8 {
    match value {
        0 => 0,
        1 => 2,
        3 => 4,
        7 => 8,
        15 => 16,
        23 => 24,
        27 => 32,
        28 => 40,
        29 => 48,
        30 => 56,
        31 => 64,
        other => todo!("{other}"),
    }
}

fn header_to_rle_v2_delta_bit_width(header: u8) -> u8 {
    let bit_width = (header & 0b00111110) >> 1;
    rle_v2_delta_bit_width(bit_width)
}

fn header_to_rle_v2_direct_length(header: u8, header1: u8) -> u16 {
    let bit = header & 0b00000001;
    let r = u16::from_be_bytes([bit, header1]);
    1 + r
}

fn unsigned_varint<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];
    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(Error::OutOfSpec);
        }
        reader.read_exact(&mut buf[..])?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }
    Ok(i)
}

#[inline]
fn zigzag(z: u64) -> i64 {
    if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    }
}

fn signed_varint<R: Read>(reader: &mut R) -> Result<i64, Error> {
    unsigned_varint(reader).map(zigzag)
}

#[inline]
fn unpack(bytes: &[u8], num_bits: u8, index: usize) -> u64 {
    if num_bits == 0 {
        return 0;
    };
    let num_bits = num_bits as usize;
    let start = num_bits * index; // in bits
    let length = num_bits; // in bits
    let byte_start = start / 8;
    let byte_end = (start + length + 7) / 8;
    // copy swapped
    let slice = &bytes[byte_start..byte_end];
    let mut a = [0u8; 8];
    for (i, item) in slice.iter().rev().enumerate() {
        a[i] = *item;
    }
    let bits = u64::from_le_bytes(a);
    let offset = (slice.len() * 8 - num_bits) % 8 - start % 8;
    (bits >> offset) & (!0u64 >> (64 - num_bits))
}

#[derive(Debug)]
pub struct UnsignedDirectRun {
    data: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
}

impl UnsignedDirectRun {
    #[inline]
    pub fn try_new<R: Read>(
        header: u8,
        reader: &mut R,
        mut scratch: Vec<u8>,
    ) -> Result<Self, Error> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1)?;
        let bit_width = header_to_rle_v2_direct_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let additional = ((bit_width as usize) * (length as usize) + 7) / 8;
        scratch.clear();
        scratch.reserve(additional);
        reader.take(additional as u64).read_to_end(&mut scratch)?;

        Ok(Self {
            data: scratch,
            bit_width,
            index: 0,
            length: length as usize,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length - self.index
    }
}

impl Iterator for UnsignedDirectRun {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            self.index += 1;
            unpack(&self.data, self.bit_width, index)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

pub struct UnsignedDeltaRun {
    encoded_deltas: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
    base: u64,
    delta_base: i64,
}

impl UnsignedDeltaRun {
    #[inline]
    pub fn try_new<R: Read>(
        header: u8,
        reader: &mut R,
        mut scratch: Vec<u8>,
    ) -> Result<Self, Error> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1)?;
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let base = unsigned_varint(reader)?;
        let delta_base = signed_varint(reader)?;
        let additional = ((length as usize - 2) * bit_width as usize + 7) / 8;

        scratch.clear();
        scratch.reserve(additional);
        reader.take(additional as u64).read_to_end(&mut scratch)?;

        Ok(Self {
            base,
            encoded_deltas: scratch,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length - self.index
    }

    #[inline]
    pub fn into_inner(mut self) -> Vec<u8> {
        self.encoded_deltas.clear();
        self.encoded_deltas
    }
}

impl Iterator for UnsignedDeltaRun {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            if index == 0 {
                self.index += 1;
                return self.base;
            }
            if index == 1 {
                self.index += 1;
                if self.delta_base > 0 {
                    self.base += self.delta_base as u64;
                } else {
                    self.base -= (-self.delta_base) as u64;
                }
                return self.base;
            }
            self.index += 1;
            let delta = unpack(&self.encoded_deltas, self.bit_width, index - 2);
            if self.delta_base > 0 {
                self.base += delta;
            } else {
                self.base -= delta;
            }
            self.base
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

#[derive(Debug)]
pub struct UnsignedShortRepeat {
    value: u64,
    remaining: usize,
    scratch: Vec<u8>,
}

impl UnsignedShortRepeat {
    #[inline]
    fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self, Error> {
        let width = 1 + header_to_rle_v2_short_repeated_width(header);
        let count = 3 + header_to_rle_v2_short_repeated_count(header);

        scratch.clear();
        scratch.reserve(width as usize);
        reader.take(width as u64).read_to_end(&mut scratch)?;

        let mut a = [0u8; 8];
        a[8 - scratch.len()..].copy_from_slice(&scratch);
        let value = u64::from_be_bytes(a);
        scratch.clear();

        Ok(Self {
            value,
            remaining: count as usize,
            scratch,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.remaining
    }

    #[inline]
    pub fn into_inner(self) -> Vec<u8> {
        self.scratch
    }
}

impl Iterator for UnsignedShortRepeat {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.remaining != 0).then(|| {
            self.remaining -= 1;
            self.value
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }
}

#[derive(Debug)]
pub struct SignedDeltaRun {
    encoded_deltas: Vec<u8>,
    bit_width: u8,
    index: usize,
    length: usize,
    base: i64,
    delta_base: i64,
}

impl SignedDeltaRun {
    #[inline]
    fn try_new<R: Read>(header: u8, reader: &mut R, mut scratch: Vec<u8>) -> Result<Self, Error> {
        let mut header1 = [0u8];
        reader.read_exact(&mut header1)?;
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, header1[0]);

        let base = unsigned_varint(reader).map(zigzag)?;
        let delta_base = signed_varint(reader)?;
        let additional = ((length as usize - 2) * bit_width as usize + 7) / 8;

        scratch.clear();
        scratch.reserve(additional);
        reader.take(additional as u64).read_to_end(&mut scratch)?;

        Ok(Self {
            base,
            encoded_deltas: scratch,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }

    pub fn len(&self) -> usize {
        self.length - self.index
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedDeltaRun {
    type Item = i64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            if index == 0 {
                self.index += 1;
                return self.base;
            }
            if index == 1 {
                self.index += 1;
                if self.delta_base > 0 {
                    self.base += self.delta_base as i64;
                } else {
                    self.base -= (-self.delta_base) as i64;
                }
                return self.base;
            }
            self.index += 1;
            let delta = unpack(&self.encoded_deltas, self.bit_width, index - 2);
            if self.delta_base > 0 {
                self.base += delta as i64;
            } else {
                self.base -= delta as i64;
            }
            self.base
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

#[inline]
fn run_encoding(header: u8) -> EncodingTypeV2 {
    match (header & 128 == 128, header & 64 == 64) {
        // 11... = 3
        (true, true) => EncodingTypeV2::Delta,
        // 10... = 2
        (true, false) => EncodingTypeV2::PatchedBase,
        // 01... = 1
        (false, true) => EncodingTypeV2::Direct,
        // 00... = 0
        (false, false) => EncodingTypeV2::ShortRepeat,
    }
}

/// An enum describing one of the RLE v2 runs for unsigned integers
pub enum UnsignedRleV2Run {
    /// Direct
    Direct(UnsignedDirectRun),
    /// Delta
    Delta(UnsignedDeltaRun),
    /// Short repeat
    ShortRepeat(UnsignedShortRepeat),
}

impl UnsignedRleV2Run {
    /// Returns a new [`UnsignedRleV2Run`] owning `scratch`.
    pub fn try_new<R: Read>(reader: &mut R, scratch: Vec<u8>) -> Result<Self, Error> {
        let mut header = [0u8];
        reader.read_exact(&mut header)?;
        let header = header[0];
        let encoding = run_encoding(header);

        match encoding {
            EncodingTypeV2::Direct => {
                UnsignedDirectRun::try_new(header, reader, scratch).map(Self::Direct)
            }
            EncodingTypeV2::Delta => {
                UnsignedDeltaRun::try_new(header, reader, scratch).map(Self::Delta)
            }
            EncodingTypeV2::ShortRepeat => {
                UnsignedShortRepeat::try_new(header, reader, scratch).map(Self::ShortRepeat)
            }
            other => todo!("{other:?}"),
        }
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        match self {
            Self::Direct(run) => run.len(),
            Self::Delta(run) => run.len(),
            Self::ShortRepeat(run) => run.len(),
        }
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A fallible [`Iterator`] of [`UnsignedRleV2Run`].
pub struct UnsignedRleV2Iter<'a, R: Read> {
    reader: &'a mut R,
    scratch: Vec<u8>,
    length: usize,
}

impl<'a, R: Read> UnsignedRleV2Iter<'a, R> {
    /// Returns a new [`UnsignedRleV2Iter`].
    pub fn new(reader: &'a mut R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            reader,
            scratch,
            length,
        }
    }
}

impl<'a, R: Read> Iterator for UnsignedRleV2Iter<'a, R> {
    type Item = Result<UnsignedRleV2Run, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.length != 0).then(|| {
            let run = UnsignedRleV2Run::try_new(self.reader, std::mem::take(&mut self.scratch))?;
            self.length -= run.len();
            Ok(run)
        })
    }
}

#[derive(Debug)]
pub struct SignedDirectRun(UnsignedDirectRun);

impl SignedDirectRun {
    pub fn try_new<R: Read>(header: u8, reader: &mut R, scratch: Vec<u8>) -> Result<Self, Error> {
        UnsignedDirectRun::try_new(header, reader, scratch).map(Self)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedDirectRun {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(zigzag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug)]
pub struct SignedShortRepeat(UnsignedShortRepeat);

impl SignedShortRepeat {
    pub fn try_new<R: Read>(header: u8, reader: &mut R, scratch: Vec<u8>) -> Result<Self, Error> {
        UnsignedShortRepeat::try_new(header, reader, scratch).map(Self)
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for SignedShortRepeat {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(zigzag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// An enum describing one of the RLE v2 runs for signed integers
#[derive(Debug)]
pub enum SignedRleV2Run {
    /// Direct
    Direct(SignedDirectRun),
    /// Delta
    Delta(SignedDeltaRun),
    /// Short repeat
    ShortRepeat(SignedShortRepeat),
}

impl SignedRleV2Run {
    /// Returns a new [`SignedRleV2Run`], moving `scratch` to itself
    pub fn try_new<R: Read>(reader: &mut R, scratch: Vec<u8>) -> Result<Self, Error> {
        let mut header = [0u8];
        reader.read_exact(&mut header)?;
        let header = header[0];
        let encoding = run_encoding(header);

        match encoding {
            EncodingTypeV2::Direct => {
                SignedDirectRun::try_new(header, reader, scratch).map(Self::Direct)
            }
            EncodingTypeV2::Delta => {
                SignedDeltaRun::try_new(header, reader, scratch).map(Self::Delta)
            }
            EncodingTypeV2::ShortRepeat => {
                SignedShortRepeat::try_new(header, reader, scratch).map(Self::ShortRepeat)
            }
            other => todo!("{other:?}"),
        }
    }

    /// The number of items remaining
    pub fn len(&self) -> usize {
        match self {
            Self::Direct(run) => run.len(),
            Self::Delta(run) => run.len(),
            Self::ShortRepeat(run) => run.len(),
        }
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A fallible [`Iterator`] of [`SignedRleV2Run`].
pub struct SignedRleV2Iter<R: Read> {
    reader: R,
    scratch: Vec<u8>,
    length: usize,
}

impl<R: Read> SignedRleV2Iter<R> {
    /// Returns a new [`SignedRleV2Iter`].
    pub fn new(reader: R, length: usize, scratch: Vec<u8>) -> Self {
        Self {
            reader,
            scratch,
            length,
        }
    }
}

impl<R: Read> Iterator for SignedRleV2Iter<R> {
    type Item = Result<SignedRleV2Run, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.length != 0).then(|| {
            let run = SignedRleV2Run::try_new(&mut self.reader, std::mem::take(&mut self.scratch))?;
            self.length -= run.len();
            Ok(run)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag(2), 1);
        assert_eq!(zigzag(4), 2);
    }

    #[test]
    fn unpacking() {
        let bytes = [0b01000000u8];
        assert_eq!(unpack(&bytes, 2, 0), 1);
        assert_eq!(unpack(&bytes, 2, 1), 0);
    }

    #[test]
    fn short_repeat() {
        // [10000, 10000, 10000, 10000, 10000]
        let data: [u8; 3] = [0x0a, 0x27, 0x10];

        let a = UnsignedShortRepeat::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![10000, 10000, 10000, 10000, 10000]);
    }

    #[test]
    fn direct() {
        // [23713, 43806, 57005, 48879]
        let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

        let data = &mut data.as_ref();

        let a = UnsignedDirectRun::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![23713, 43806, 57005, 48879]);
    }

    #[test]
    fn delta() {
        // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        // 0x22 = 34
        // 0x42 = 66
        // 0x46 = 70
        let data: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

        let data = &mut data.as_ref();

        let a = UnsignedDeltaRun::try_new(data[0], &mut &data[1..], vec![])
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(a, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
    }
}
