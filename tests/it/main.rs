use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use orc_format::{proto::stream::Kind, read};

#[test]
fn read_schema() {
    let mut f = File::open(&"test.orc").expect("no file found");

    let (ps, footer, metadata) = read::read_metadata(&mut f).unwrap();
    println!("{:#?}", footer);
    println!("{:#?}", metadata);

    println!("{:#?}", footer.stripes);
    for stripe_info in footer.stripes {
        let a = stripe_info.offset();
        f.seek(SeekFrom::Start(a)).unwrap();

        let len =
            stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
        let mut stripe = vec![0; len as usize];
        f.read_exact(&mut stripe).unwrap();

        //let stripe = read::read_stripe_footer(&buffer).unwrap();
        let footer_offset = (stripe_info.index_length() + stripe_info.data_length()) as usize;
        let stripe_footer = read::deserialize_stripe_footer(&stripe[footer_offset..]).unwrap();
        println!("{:#?}", stripe_footer);

        let lengths: Vec<u64> = stripe_footer.streams.iter().fold(vec![0], |mut acc, v| {
            acc.push(acc.last().copied().unwrap() + v.length());
            acc
        });

        let get_bytes = |column: u32| -> Vec<(u64, u64)> {
            stripe_footer
                .streams
                .iter()
                .zip(lengths.windows(2))
                .filter(|(stream, _)| {
                    stream.column == Some(column) && stream.kind() != Kind::RowIndex
                })
                .map(|(_, length)| {
                    (
                        stripe_info.index_length() + length[0],
                        length[1] - length[0],
                    )
                })
                .collect()
        };

        let a = get_bytes(1);

        let data = &stripe[a[0].0 as usize..a[0].0 as usize + a[0].1 as usize];
        let header = data[0];
        println!("{header:b}");
    }
}
