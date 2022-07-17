use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use orc_format::{
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::Stripe,
    Error,
};

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

fn signed_varint<R: std::io::Read>(reader: &mut R) -> Result<i64, Error> {
    let z = unsigned_varint(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

#[inline]
fn unpack(bytes: &[u8], num_bits: u8, index: usize) -> u64 {
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

fn short_repeat_u64(data: &[u8]) -> impl Iterator<Item = u64> + '_ {
    let header = data[0];
    let width = 1 + header_to_rle_v2_short_repeated_width(header);
    let count = 3 + header_to_rle_v2_short_repeated_count(header);
    let data = &data[1..1 + width as usize];
    let mut a = [0u8; 8];
    a[8 - data.len()..].copy_from_slice(data);
    let value = u64::from_be_bytes(a);
    std::iter::repeat(value).take(count as usize)
}

fn direct_u64(data: &[u8]) -> impl Iterator<Item = u64> + '_ {
    let header = data[0];
    let bit_width = header_to_rle_v2_direct_bit_width(header);

    let length = header_to_rle_v2_direct_length(header, data[1]);
    let data = &data[2..];

    let remaining = ((bit_width as usize) * (length as usize) + 7) / 8;
    let data = &data[..remaining];

    (0..length as usize).map(move |x| unpack(data, bit_width, x))
}

fn delta_u64(data: &[u8]) -> Result<impl Iterator<Item = u64> + '_, Error> {
    let header = data[0];
    let bit_width = header_to_rle_v2_delta_bit_width(header);

    let length = header_to_rle_v2_direct_length(header, data[1]);
    let mut data = &data[2..];

    let reader = &mut data;
    let value = unsigned_varint(reader)?;
    let delta_base = signed_varint(reader)?;
    data = reader;

    let deltas = (0..length as usize - 2).map(move |index| unpack(data, bit_width, index));

    let mut base = value;
    if delta_base > 0 {
        base += delta_base as u64;
    } else {
        base -= (-delta_base) as u64;
    }
    Ok(std::iter::once(value)
        .chain(std::iter::once(base))
        .chain(deltas.map(move |delta| {
            if delta_base > 0 {
                base += delta;
            } else {
                base -= delta;
            }
            base
        })))
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

    let a = short_repeat_u64(&data).collect::<Vec<_>>();
    assert_eq!(a, vec![10000, 10000, 10000, 10000, 10000]);
}

#[test]
fn direct() {
    // [23713, 43806, 57005, 48879]
    let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

    let a = direct_u64(&data).collect::<Vec<_>>();
    assert_eq!(a, vec![23713, 43806, 57005, 48879]);
}

#[test]
fn delta() {
    // [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    // 0x22 = 34
    // 0x42 = 66
    // 0x46 = 70
    let data: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

    let a = delta_u64(&data).unwrap().collect::<Vec<_>>();
    assert_eq!(a, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
}

enum IteratorEnum<I, II, III> {
    Direct(I),
    Delta(II),
    ShortRepeat(III),
}

fn unsigned(
    data: &[u8],
) -> Result<
    IteratorEnum<
        impl Iterator<Item = u64> + '_,
        impl Iterator<Item = u64> + '_,
        impl Iterator<Item = u64> + '_,
    >,
    Error,
> {
    let header = data[0];
    let encoding = match (header & 128 == 128, header & 64 == 64) {
        // 11... = 3
        (true, true) => EncodingTypeV2::Delta,
        // 10... = 2
        (true, false) => EncodingTypeV2::PatchedBase,
        // 01... = 1
        (false, true) => EncodingTypeV2::Direct,
        // 00... = 0
        (false, false) => EncodingTypeV2::ShortRepeat,
    };

    Ok(match encoding {
        EncodingTypeV2::Direct => IteratorEnum::Direct(direct_u64(data)),
        EncodingTypeV2::Delta => IteratorEnum::Delta(delta_u64(data)?),
        EncodingTypeV2::ShortRepeat => IteratorEnum::ShortRepeat(short_repeat_u64(data)),
        other => todo!("{other:?}"),
    })
}

fn deserialize_f32(stream: &[u8]) -> impl Iterator<Item = f32> + '_ {
    stream
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
}

fn deserialize_f32_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<f32>), Error> {
    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present)?;

    let iter = read::decode::BooleanRleIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let data = stripe.get_bytes(column, Kind::Data)?;

    let valid_values = deserialize_f32(data).collect::<Vec<_>>();
    Ok((validity, valid_values))
}

fn deserialize_bool(
    stream: &[u8],
    num_of_rows: usize,
) -> impl Iterator<Item = Result<bool, Error>> + '_ {
    read::decode::BooleanRleIter::new(stream, num_of_rows)
}

fn deserialize_bool_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present)?;

    let iter = read::decode::BooleanRleIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let num_valids = validity.iter().filter(|x| **x).count();

    let data = stripe.get_bytes(column, Kind::Data)?;

    let valid_values = deserialize_bool(data, num_valids).collect::<Result<Vec<_>, Error>>()?;
    Ok((validity, valid_values))
}

fn deserialize_str<'a>(
    mut values: &'a [u8],
    lengths: &'a [u8],
) -> Result<
    IteratorEnum<
        impl Iterator<Item = Result<&'a str, Error>> + 'a,
        impl Iterator<Item = Result<&'a str, Error>> + 'a,
        impl Iterator<Item = Result<&'a str, Error>> + 'a,
    >,
    Error,
> {
    let f = move |length| {
        let (item, remaining) = values.split_at(length as usize);
        values = remaining;
        std::str::from_utf8(item).map_err(|_| Error::InvalidUtf8)
    };

    Ok(match unsigned(lengths)? {
        IteratorEnum::Direct(values) => IteratorEnum::Direct(values.map(f)),
        IteratorEnum::Delta(values) => IteratorEnum::Delta(values.map(f)),
        IteratorEnum::ShortRepeat(values) => IteratorEnum::ShortRepeat(values.map(f)),
    })
}

fn deserialize_str_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<&str>), Error> {
    let num_of_rows = stripe.number_of_rows();

    let data = stripe.get_bytes(column, Kind::Present)?;
    let iter = read::decode::BooleanRleIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    // todo: generalize to other encodings
    let encoding = stripe.get_encoding(column)?;
    assert_eq!(encoding.kind(), ColumnEncodingKind::DirectV2);

    let values = stripe.get_bytes(column, Kind::Data)?;

    let lengths = stripe.get_bytes(column, Kind::Length)?;

    let valid_values = match deserialize_str(values, lengths)? {
        IteratorEnum::Direct(values) => values.collect::<Result<Vec<_>, Error>>()?,
        IteratorEnum::Delta(values) => values.collect::<Result<Vec<_>, Error>>()?,
        IteratorEnum::ShortRepeat(values) => values.collect::<Result<Vec<_>, Error>>()?,
    };
    Ok((validity, valid_values))
}

fn get_test_stripe() -> Result<Stripe, Error> {
    let mut f = File::open(&"test.orc").expect("no file found");

    let (ps, mut footer, _metadata) = read::read_metadata(&mut f)?;

    let stripe_info = footer.stripes.pop().unwrap();

    let a = stripe_info.offset();
    f.seek(SeekFrom::Start(a)).unwrap();

    let len = stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
    let mut stripe = vec![0; len as usize];
    f.read_exact(&mut stripe).unwrap();

    Stripe::try_new(stripe, stripe_info, ps.compression())
}

#[test]
fn read_bool() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_bool_array(&stripe, 2)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![true, false, true, false]);
    Ok(())
}

#[test]
fn read_str_direct() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_str_array(&stripe, 3)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "cccccc", "ddd", "ee"]);
    Ok(())
}

#[test]
fn read_str_delta_plus() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_str_array(&stripe, 4)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "bb", "ccc", "ddd"]);
    Ok(())
}

#[test]
fn read_str_delta_minus() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_str_array(&stripe, 5)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["ddd", "cc", "bb", "a"]);
    Ok(())
}

#[test]
fn read_str_short_repeat() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_str_array(&stripe, 6)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["aaaaa", "bbbbb", "ccccc", "ddddd"]);
    Ok(())
}

#[test]
fn read_f32() -> Result<(), Error> {
    let stripe = get_test_stripe()?;

    let (a, b) = deserialize_f32_array(&stripe, 1)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1.0, 2.0, 4.0, 5.0]);
    Ok(())
}
