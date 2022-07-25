use crate::Error;

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

fn unsigned_varint<R: std::io::Read>(reader: &mut R) -> Result<u64, Error> {
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

fn signed_varint<R: std::io::Read>(reader: &mut R) -> Result<i64, Error> {
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

pub struct UnsignedDirectRun<'a> {
    data: &'a [u8],
    bit_width: u8,
    index: usize,
    length: usize,
}

impl<'a> UnsignedDirectRun<'a> {
    #[inline]
    fn new(data: &mut &'a [u8]) -> Self {
        let header = data[0];
        let bit_width = header_to_rle_v2_direct_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, data[1]);
        let run_bytes = &data[2..];

        let remaining = ((bit_width as usize) * (length as usize) + 7) / 8;
        let run = &run_bytes[..remaining];
        *data = &run_bytes[remaining..];

        Self {
            data: run,
            bit_width,
            index: 0,
            length: length as usize,
        }
    }
}

impl<'a> Iterator for UnsignedDirectRun<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        (self.index != self.length).then(|| {
            let index = self.index;
            self.index += 1;
            unpack(self.data, self.bit_width, index)
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

pub struct UnsignedDeltaRun<'a> {
    encoded_deltas: &'a [u8],
    bit_width: u8,
    index: usize,
    length: usize,
    base: u64,
    delta_base: i64,
}

impl<'a> UnsignedDeltaRun<'a> {
    #[inline]
    fn try_new(data: &mut &'a [u8]) -> Result<Self, Error> {
        let header = data[0];
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, data[1]);
        *data = &data[2..];

        let base = unsigned_varint(data)?;
        let delta_base = signed_varint(data)?;
        let remaining = ((length as usize - 2) * bit_width as usize + 7) / 8;
        let encoded_deltas = &data[..remaining];
        *data = &data[remaining..];

        Ok(Self {
            base,
            encoded_deltas,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }
}

impl<'a> Iterator for UnsignedDeltaRun<'a> {
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
            let delta = unpack(self.encoded_deltas, self.bit_width, index - 2);
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
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

pub struct UnsignedShortRepeat {
    value: u64,
    remaining: usize,
}

impl UnsignedShortRepeat {
    #[inline]
    fn new(data: &mut &[u8]) -> Self {
        let header = data[0];
        let width = 1 + header_to_rle_v2_short_repeated_width(header);
        let count = 3 + header_to_rle_v2_short_repeated_count(header);
        let inner = &data[1..1 + width as usize];
        *data = &data[1 + width as usize..];
        let mut a = [0u8; 8];
        a[8 - inner.len()..].copy_from_slice(inner);
        let value = u64::from_be_bytes(a);

        Self {
            value,
            remaining: count as usize,
        }
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
        (self.remaining, Some(self.remaining))
    }
}

pub struct SignedDeltaRun<'a> {
    encoded_deltas: &'a [u8],
    bit_width: u8,
    index: usize,
    length: usize,
    base: i64,
    delta_base: i64,
}

impl<'a> SignedDeltaRun<'a> {
    #[inline]
    fn try_new(data: &mut &'a [u8]) -> Result<Self, Error> {
        let header = data[0];
        let bit_width = header_to_rle_v2_delta_bit_width(header);

        let length = header_to_rle_v2_direct_length(header, data[1]);
        *data = &data[2..];

        let base = unsigned_varint(data).map(zigzag)?;
        let delta_base = signed_varint(data)?;
        let remaining = ((length as usize - 2) * bit_width as usize + 7) / 8;
        let encoded_deltas = &data[..remaining];
        *data = &data[remaining..];

        Ok(Self {
            base,
            encoded_deltas,
            bit_width,
            index: 0,
            length: length as usize,
            delta_base,
        })
    }
}

impl<'a> Iterator for SignedDeltaRun<'a> {
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
            let delta = unpack(self.encoded_deltas, self.bit_width, index - 2);
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

pub enum IteratorEnum<I, II, III> {
    Direct(I),
    Delta(II),
    ShortRepeat(III),
}

fn run_encoding(data: &[u8]) -> EncodingTypeV2 {
    let header = data[0];
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

pub enum UnsignedRleV2Run<'a> {
    Direct(UnsignedDirectRun<'a>),
    Delta(UnsignedDeltaRun<'a>),
    ShortRepeat(UnsignedShortRepeat),
}

impl<'a> UnsignedRleV2Run<'a> {
    pub fn try_new(data: &mut &'a [u8]) -> Result<Self, Error> {
        let encoding = run_encoding(data);

        Ok(match encoding {
            EncodingTypeV2::Direct => Self::Direct(UnsignedDirectRun::new(data)),
            EncodingTypeV2::Delta => Self::Delta(UnsignedDeltaRun::try_new(data)?),
            EncodingTypeV2::ShortRepeat => Self::ShortRepeat(UnsignedShortRepeat::new(data)),
            other => todo!("{other:?}"),
        })
    }
}

pub struct UnsignedRleV2Iter<'a> {
    data: &'a [u8],
}

impl<'a> UnsignedRleV2Iter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for UnsignedRleV2Iter<'a> {
    type Item = Result<UnsignedRleV2Run<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        (!self.data.is_empty()).then(|| {
            let stream = &mut self.data;
            let run = UnsignedRleV2Run::try_new(stream);
            self.data = *stream;
            run
        })
    }
}

pub struct SignedDirectRun<'a>(UnsignedDirectRun<'a>);

impl<'a> SignedDirectRun<'a> {
    pub fn new(data: &mut &'a [u8]) -> Self {
        Self(UnsignedDirectRun::new(data))
    }
}

impl<'a> Iterator for SignedDirectRun<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(zigzag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub struct SignedShortRepeat(UnsignedShortRepeat);

impl SignedShortRepeat {
    pub fn new(data: &mut &[u8]) -> Self {
        Self(UnsignedShortRepeat::new(data))
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

pub enum SignedRleV2Run<'a> {
    Direct(SignedDirectRun<'a>),
    Delta(SignedDeltaRun<'a>),
    ShortRepeat(SignedShortRepeat),
}

impl<'a> SignedRleV2Run<'a> {
    pub fn try_new(data: &mut &'a [u8]) -> Result<Self, Error> {
        let encoding = run_encoding(data);

        Ok(match encoding {
            EncodingTypeV2::Direct => Self::Direct(SignedDirectRun::new(data)),
            EncodingTypeV2::Delta => Self::Delta(SignedDeltaRun::try_new(data)?),
            EncodingTypeV2::ShortRepeat => Self::ShortRepeat(SignedShortRepeat::new(data)),
            other => todo!("{other:?}"),
        })
    }
}

pub struct SignedRleV2Iter<'a> {
    data: &'a [u8],
}

impl<'a> SignedRleV2Iter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for SignedRleV2Iter<'a> {
    type Item = Result<SignedRleV2Run<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        (!self.data.is_empty()).then(|| {
            let stream = &mut self.data;
            let run = SignedRleV2Run::try_new(stream);
            self.data = *stream;
            run
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

        let data = &mut data.as_ref();

        let a = UnsignedShortRepeat::new(data).collect::<Vec<_>>();
        assert_eq!(a, vec![10000, 10000, 10000, 10000, 10000]);
        assert_eq!(data, &[]);
    }

    #[test]
    fn direct() {
        // [23713, 43806, 57005, 48879]
        let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

        let data = &mut data.as_ref();

        let a = UnsignedDirectRun::new(data).collect::<Vec<_>>();
        assert_eq!(a, vec![23713, 43806, 57005, 48879]);
        assert_eq!(data, &[]);
    }

    #[test]
    fn delta() {
        // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        // 0x22 = 34
        // 0x42 = 66
        // 0x46 = 70
        let data: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

        let data = &mut data.as_ref();

        let a = UnsignedDeltaRun::try_new(data).unwrap().collect::<Vec<_>>();
        assert_eq!(a, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
        assert_eq!(data, &[]);
    }
}
