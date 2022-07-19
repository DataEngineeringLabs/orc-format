use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

mod deserialize;
use deserialize::*;

use orc_format::{read, read::Stripe, Error};

fn get_test_stripe(path: &str) -> Result<Stripe, Error> {
    let mut f = File::open(path).expect("no file found");

    let (ps, mut footer, _metadata) = read::read_metadata(&mut f)?;

    let stripe_info = footer.stripes.pop().unwrap();

    let a = stripe_info.offset();
    f.seek(SeekFrom::Start(a)).unwrap();

    let len = stripe_info.index_length() + stripe_info.data_length() + stripe_info.footer_length();
    let mut stripe = vec![0; len as usize];
    f.read_exact(&mut stripe).unwrap();
    println!("{:?}", ps.compression());

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

#[test]
fn read_bool_compressed() -> Result<(), Error> {
    let stripe = get_test_stripe("long_bool_gzip.orc")?;

    let (a, b) = deserialize_bool_array(&stripe, 1)?;
    assert_eq!(a, vec![true; 32]);
    assert_eq!(b, vec![true; 32]);
    Ok(())
}

#[test]
fn read_string_long() -> Result<(), Error> {
    let stripe = get_test_stripe("string_long.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 1)?;
    assert_eq!(a, vec![true; 64]);
    assert_eq!(
        b,
        vec!["abcd", "efgh"]
            .into_iter()
            .cycle()
            .take(64)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn read_string_dict() -> Result<(), Error> {
    let stripe = get_test_stripe("string_dict.orc")?;

    let (a, b) = deserialize_str_array(&stripe, 1)?;
    assert_eq!(a, vec![true; 64]);
    assert_eq!(
        b,
        vec!["abc", "efgh"]
            .into_iter()
            .cycle()
            .take(64)
            .collect::<Vec<_>>()
    );
    Ok(())
}
