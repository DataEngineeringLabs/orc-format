use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use orc_format::{
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind, ColumnEncoding},
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

fn header_to_rle_v2_bit_width(header: u8) -> u8 {
    let header = header >> 2; // remove first 2 bits
    todo!()
}

fn header_to_rle_v2_short_repeated_bit_width(header: u8) -> u8 {
    let header = header << 2; // remove first 2 bits
    let bit_width = header & 0b11100000;
    u8::from_le_bytes([bit_width])
}

fn header_to_rle_v2_short_repeated_count(header: u8) -> u8 {
    let header = header << 5; // remove first 5 bits
    let count = header & 0b11100000;
    todo!()
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

fn header_to_rle_v2_direct_length(header: u8, header1: u8) -> u16 {
    let bit = header & 0b00000001;
    let r = u16::from_be_bytes([bit, header1]);
    1 + r
}

fn direct_u64(data: &[u8]) -> impl Iterator<Item = u64> + '_ {
    let header = data[0];
    let bit_width = header_to_rle_v2_direct_bit_width(header);
    dbg!(bit_width);

    let length = header_to_rle_v2_direct_length(header, data[1]);
    dbg!(length);
    let data = &data[2..];

    let remaining = ((bit_width as usize) * (length as usize) + 7) / 8;
    let data = &data[..remaining];
    dbg!(data);

    let bytes = ((bit_width + 7) / 8) as usize;

    let packs = (remaining + 7) / 8;
    println!("{packs}");
    for pack in 0..packs {
        let mut pack = [0u8; 8];
        pack[..data.len()].copy_from_slice(data);
        let a = u64::from_le_bytes(pack);
        println!("{:#066b}", a);

        let mut pack = [0u64; 8];
        for item in 0..8 {
            let mask = pack[0] = a & mask;
        }
    }

    let remainder = [0u8; 8];
    (0..length as usize).map(move |i| {
        // todo: apply carry-over from remainder;
        let data = &data[i * bytes..(i + 1) * bytes];
        let mut a = [0u8; 8];
        a[8 - bytes..].copy_from_slice(data);
        u64::from_be_bytes(a)
    })
}

#[test]
fn direct() {
    // [23713, 43806, 57005, 48879]
    let data: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];

    let a = direct_u64(&data).collect::<Vec<_>>();
    assert_eq!(a, vec![23713, 43806, 57005, 48879]);
}

fn integers(data: &[u8]) -> impl Iterator<Item = u64> + '_ {
    // INTEGERS
    // 10, 39, 16
    // header: 0b00001010
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

    assert_eq!(encoding, EncodingTypeV2::Direct);
    direct_u64(data)
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

    let data = stripe.get_bytes(column, Kind::Data)?;

    let valid_values = deserialize_bool(data, num_of_rows).collect::<Result<Vec<_>, Error>>()?;
    Ok((validity, valid_values))
}

fn deserialize_string(values: &[u8], lengths: &[u8], num_of_rows: usize) -> Result<(), Error> {
    println!("{lengths:?}");
    integers(lengths).collect::<Vec<_>>();
    todo!()
}

fn deserialize_string_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<String>), Error> {
    let num_of_rows = stripe.number_of_rows();

    //let data = stripe.get_bytes(column, Kind::Present)?;
    //let iter = read::decode::BooleanRleIter::new(data, num_of_rows);
    //let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let encoding = stripe.get_encoding(column)?;

    let values = stripe.get_bytes(column, Kind::Data)?;

    let lengths = stripe.get_bytes(column, Kind::Length)?;

    assert_eq!(encoding.kind(), ColumnEncodingKind::DirectV2);

    let valid_values = deserialize_string(values, lengths, num_of_rows)?;
    todo!()
}

#[test]
fn read_basics() -> Result<(), Error> {
    let mut f = File::open(&"test.orc").expect("no file found");

    let (ps, footer, metadata) = read::read_metadata(&mut f)?;

    println!("{:#?}", footer.types);

    for stripe_info in footer.stripes {
        let a = stripe_info.offset();
        f.seek(SeekFrom::Start(a)).unwrap();

        let len =
            stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
        let mut stripe = vec![0; len as usize];
        f.read_exact(&mut stripe).unwrap();

        let stripe = Stripe::try_new(&stripe, stripe_info, ps.compression())?;

        /*
        let (a, b) = deserialize_f32_array(&stripe, 1)?;
        assert_eq!(a, vec![true; 5]);
        assert_eq!(b, vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        let (a, b) = deserialize_bool_array(&stripe, 2)?;
        assert_eq!(a, vec![true, true, false, true, true]);
        assert_eq!(b, vec![true, false, true, false, false]); // +1 element due to nulls
         */

        deserialize_string_array(&stripe, 3)?;
    }

    Ok(())
}
