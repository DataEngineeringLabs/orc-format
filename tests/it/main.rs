use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use orc_format::{proto::stream::Kind, read, Error};

/*
unsigned char reverse(unsigned char b) {
   b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
   b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
   b = (b & 0xAA) >> 1 | (b & 0x55) << 1;
   return b;
}
*/

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
    println!("{:#010b}", header);
    let header = header << 2; // remove first 2 bits
    println!("{:#010b}", header & 0b11100000);
    let bit_width = header & 0b11100000;
    u8::from_le_bytes([bit_width])
}

fn header_to_rle_v2_short_repeated_count(header: u8) -> u8 {
    println!("{:#010b}", header);
    let header = header << 5; // remove first 5 bits
    let count = header & 0b11100000;
    println!("{:#010b}", count);
    println!("{}", count);
    todo!()
}

fn integers(data: &[u8]) {
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

    assert_eq!(encoding, EncodingTypeV2::ShortRepeat);
    let bit_width = header_to_rle_v2_short_repeated_bit_width(header);
    dbg!(bit_width);
    header_to_rle_v2_short_repeated_count(header);
}

fn deserialize_f32(stream: &[u8]) -> impl Iterator<Item = f32> + '_ {
    stream
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
}

fn deserialize_f64(stream: &[u8]) -> impl Iterator<Item = f64> + '_ {
    stream
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
}

#[test]
fn read_schema() {
    let mut f = File::open(&"test.orc").expect("no file found");

    let (ps, footer, metadata) = read::read_metadata(&mut f).unwrap();

    println!("{:#?}", footer);
    println!("{:#?}", metadata);

    for stripe_info in footer.stripes {
        let a = stripe_info.offset();
        f.seek(SeekFrom::Start(a)).unwrap();
        println!("{:?}", stripe_info);
        println!("{:?}", stripe_info.index_length());
        println!("{:?}", stripe_info.data_length());
        println!("{:?}", stripe_info.footer_length());

        let len =
            stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
        let mut stripe = vec![0; len as usize];
        f.read_exact(&mut stripe).unwrap();
        println!("{stripe:?}");

        //let stripe = read::read_stripe_footer(&buffer).unwrap();
        let footer_offset = (stripe_info.index_length() + stripe_info.data_length()) as usize;
        let stripe_footer =
            read::deserialize_stripe_footer(&stripe[footer_offset..], ps.compression()).unwrap();
        println!("{:#?}", stripe_footer);

        let offsets: Vec<u64> = stripe_footer.streams.iter().fold(vec![0], |mut acc, v| {
            acc.push(acc.last().copied().unwrap() + v.length());
            acc
        });

        let num_of_rows = stripe_info.number_of_rows() as usize;

        println!("{offsets:?}");

        let get_bytes = |column: u32, kind: Kind| -> &[u8] {
            stripe_footer
                .streams
                .iter()
                .zip(offsets.windows(2))
                .filter(|(stream, _)| stream.column == Some(column) && stream.kind() == kind)
                .map(|(stream, offsets)| {
                    let start = offsets[0];
                    debug_assert_eq!(offsets[1] - offsets[0], stream.length());
                    let length = stream.length();
                    println!("{start} {length}");
                    &stripe[start as usize..(start + length) as usize]
                })
                .next()
                .unwrap()
        };

        let data = get_bytes(1, Kind::Present);

        let iter = read::decode::BooleanRleIter::new(data, num_of_rows);
        let validity = iter.collect::<Result<Vec<_>, Error>>().unwrap();

        let data = get_bytes(1, Kind::Data);

        let valid_values = deserialize_f32(data).collect::<Vec<_>>();
        println!("{:?}", validity);
        println!("{:?}", valid_values);
    }
}
