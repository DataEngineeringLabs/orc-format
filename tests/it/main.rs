use std::fs::File;

mod deserialize;
use deserialize::*;

use orc_format::{error::Error, read, read::Column};

fn get_column(path: &str, column: u32) -> Result<Column, Error> {
    // open the file, as expected. buffering this is not necessary - we
    // are very careful about the number of `read`s we perform.
    let mut f = File::open(path).expect("no file found");

    // read the files' metadata
    let metadata = read::read_metadata(&mut f)?;
    // and copy the compression it is using
    let compression = metadata.postscript.compression();

    // the next step is to identify which stripe we want to read. Let's say it is the first one.
    let stripe = &metadata.footer.stripes[0];

    // Each stripe has a footer - we need to read it to extract the location of each column on it.
    let stripe_footer = read::read_stripe_footer(&mut f, stripe, compression, &mut vec![])?;

    // Finally, we read the column into `Column`
    read::read_stripe_column(&mut f, stripe, stripe_footer, compression, column, vec![])
}

#[test]
fn read_bool() -> Result<(), Error> {
    let column = get_column("test.orc", 2)?;

    let (a, b) = deserialize_bool_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![true, false, true, false]);
    Ok(())
}

#[test]
fn read_str_direct() -> Result<(), Error> {
    let column = get_column("test.orc", 3)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "cccccc", "ddd", "ee"]);
    Ok(())
}

#[test]
fn read_str_delta_plus() -> Result<(), Error> {
    let column = get_column("test.orc", 4)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["a", "bb", "ccc", "ddd"]);
    Ok(())
}

#[test]
fn read_str_delta_minus() -> Result<(), Error> {
    let column = get_column("test.orc", 5)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["ddd", "cc", "bb", "a"]);
    Ok(())
}

#[test]
fn read_str_short_repeat() -> Result<(), Error> {
    let column = get_column("test.orc", 6)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec!["aaaaa", "bbbbb", "ccccc", "ddddd"]);
    Ok(())
}

#[test]
fn read_f32() -> Result<(), Error> {
    let column = get_column("test.orc", 1)?;

    let (a, b) = deserialize_f32_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1.0, 2.0, 4.0, 5.0]);
    Ok(())
}

#[test]
fn read_int_short_repeated() -> Result<(), Error> {
    let column = get_column("test.orc", 7)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![5, 5, 5, 5]);
    Ok(())
}

#[test]
fn read_int_neg_short_repeated() -> Result<(), Error> {
    let column = get_column("test.orc", 8)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![-5, -5, -5, -5]);
    Ok(())
}

#[test]
fn read_int_delta() -> Result<(), Error> {
    let column = get_column("test.orc", 9)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 2, 4, 5]);
    Ok(())
}

#[test]
fn read_int_neg_delta() -> Result<(), Error> {
    let column = get_column("test.orc", 10)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![5, 4, 2, 1]);
    Ok(())
}

#[test]
fn read_int_direct() -> Result<(), Error> {
    let column = get_column("test.orc", 11)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 6, 3, 2]);
    Ok(())
}

#[test]
fn read_int_neg_direct() -> Result<(), Error> {
    let column = get_column("test.orc", 11)?;

    let (a, b) = deserialize_int_array(&column)?;
    assert_eq!(a, vec![true, true, false, true, true]);
    assert_eq!(b, vec![1, 6, 3, 2]);
    Ok(())
}

#[test]
fn read_boolean_long() -> Result<(), Error> {
    let column = get_column("long_bool.orc", 1)?;

    let (a, b) = deserialize_bool_array(&column)?;
    assert_eq!(a, vec![true; 32]);
    assert_eq!(b, vec![true; 32]);
    Ok(())
}

#[test]
fn read_bool_compressed() -> Result<(), Error> {
    let column = get_column("long_bool_gzip.orc", 1)?;

    let (a, b) = deserialize_bool_array(&column)?;
    assert_eq!(a, vec![true; 32]);
    assert_eq!(b, vec![true; 32]);
    Ok(())
}

#[test]
fn read_string_long() -> Result<(), Error> {
    let column = get_column("string_long.orc", 1)?;

    let (a, b) = deserialize_str_array(&column)?;
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
    let column = get_column("string_dict.orc", 1)?;

    let (a, b) = deserialize_str_array(&column)?;
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

#[test]
fn read_string_dict_gzip() -> Result<(), Error> {
    let column = get_column("string_dict_gzip.orc", 1)?;

    let (a, b) = deserialize_str_array(&column)?;
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

#[test]
fn read_string_long_long() -> Result<(), Error> {
    let column = get_column("string_long_long.orc", 1)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a.len(), 10_000);
    assert_eq!(a, vec![true; 10_000]);
    assert_eq!(b.len(), 10_000);
    assert_eq!(
        b,
        vec!["abcd", "efgh"]
            .into_iter()
            .cycle()
            .take(10_000)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn read_string_long_long_gzip() -> Result<(), Error> {
    let column = get_column("string_long_long_gzip.orc", 1)?;

    let (a, b) = deserialize_str_array(&column)?;
    assert_eq!(a.len(), 10_000);
    assert_eq!(a, vec![true; 10_000]);
    assert_eq!(b.len(), 10_000);
    assert_eq!(
        b,
        vec!["abcd", "efgh"]
            .into_iter()
            .cycle()
            .take(10_000)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn read_f32_long_long_gzip() -> Result<(), Error> {
    let column = get_column("f32_long_long_gzip.orc", 1)?;

    let (a, b) = deserialize_f32_array(&column)?;
    assert_eq!(a.len(), 1_000_000);
    assert_eq!(a, vec![true; 1_000_000]);
    assert_eq!(b.len(), 1_000_000);
    Ok(())
}
