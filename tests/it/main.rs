use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use orc_format::{
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::decode::{BooleanIter, IteratorEnum},
    read::Stripe,
    Error,
};

fn deserialize_f32_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<f32>), Error> {
    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present)?;

    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let data = stripe.get_bytes(column, Kind::Data)?;

    let valid_values = read::decode::deserialize_f32(data).collect::<Vec<_>>();
    Ok((validity, valid_values))
}

fn deserialize_int_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<i64>), Error> {
    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present)?;

    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let data = stripe.get_bytes(column, Kind::Data)?;

    let valid_values = match read::decode::v2_signed(data)? {
        IteratorEnum::Direct(values) => values.collect::<Vec<_>>(),
        IteratorEnum::Delta(values) => values.collect::<Vec<_>>(),
        IteratorEnum::ShortRepeat(values) => values.collect::<Vec<_>>(),
    };

    Ok((validity, valid_values))
}

fn deserialize_bool(
    stream: &[u8],
    num_of_rows: usize,
) -> impl Iterator<Item = Result<bool, Error>> + '_ {
    BooleanIter::new(stream, num_of_rows)
}

fn deserialize_bool_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present)?;

    let iter = BooleanIter::new(data, num_of_rows);
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

    Ok(match read::decode::v2_unsigned(lengths)? {
        IteratorEnum::Direct(values) => IteratorEnum::Direct(values.map(f)),
        IteratorEnum::Delta(values) => IteratorEnum::Delta(values.map(f)),
        IteratorEnum::ShortRepeat(values) => IteratorEnum::ShortRepeat(values.map(f)),
    })
}

fn deserialize_str_array(stripe: &Stripe, column: usize) -> Result<(Vec<bool>, Vec<&str>), Error> {
    let num_of_rows = stripe.number_of_rows();

    let data = stripe.get_bytes(column, Kind::Present)?;
    let iter = BooleanIter::new(data, num_of_rows);
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

fn get_test_stripe(path: &str) -> Result<Stripe, Error> {
    let mut f = File::open(path).expect("no file found");

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
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_bool_array(&stripe, 2)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![true, false, true, false]);
    Ok(())
}

#[test]
fn read_str_direct() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 3)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "cccccc", "ddd", "ee"]);
    Ok(())
}

#[test]
fn read_str_delta_plus() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 4)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "bb", "ccc", "ddd"]);
    Ok(())
}

#[test]
fn read_str_delta_minus() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 5)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["ddd", "cc", "bb", "a"]);
    Ok(())
}

#[test]
fn read_str_short_repeat() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 6)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["aaaaa", "bbbbb", "ccccc", "ddddd"]);
    Ok(())
}

#[test]
fn read_f32() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_f32_array(&stripe, 1)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1.0, 2.0, 4.0, 5.0]);
    Ok(())
}

#[test]
fn read_int_short_repeated() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 7)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![5, 5, 5, 5]);
    Ok(())
}

#[test]
fn read_int_neg_short_repeated() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 8)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![-5, -5, -5, -5]);
    Ok(())
}

#[test]
fn read_int_delta() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 9)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 2, 4, 5]);
    Ok(())
}

#[test]
fn read_int_neg_delta() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 10)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![5, 4, 2, 1]);
    Ok(())
}

#[test]
fn read_int_direct() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 11)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 6, 3, 2]);
    Ok(())
}

#[test]
fn read_int_neg_direct() -> Result<(), Error> {
    let stripe = get_test_stripe("test.orc")?;

    let (a, b) = deserialize_int_array(&stripe, 11)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 6, 3, 2]);
    Ok(())
}

#[test]
fn read_boolean_long() -> Result<(), Error> {
    let stripe = get_test_stripe("long_bool.orc")?;

    let (a, b) = deserialize_bool_array(&stripe, 1)?;
    assert_eq!(a, vec![true; 32]);
    assert_eq!(b, vec![true; 32]);
    Ok(())
}
