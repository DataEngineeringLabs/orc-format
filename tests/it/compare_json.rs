use std::fmt::Debug;
use std::io::{Cursor, Read};

use json::{number::Number, JsonValue};
use orc_format::{
    proto::r#type::Kind, proto::StripeFooter, read::read_metadata, read::read_stripe_column,
    read::read_stripe_footer, read::FileMetadata,
};

use super::deserialize::*;

macro_rules! test_apache(
    ($test_name:ident, $file_name: tt) => {
        #[test]
        #[allow(non_snake_case)]
        fn $test_name() {
            let orc_data = include_bytes!(concat!("../examples/", $file_name, ".orc"));
            let jsn_gz_data: &[u8] = include_bytes!(concat!("../examples/expected/", $file_name, ".jsn.gz"));
            inner_test(orc_data, jsn_gz_data);
        }
    };
    ($name_prefix:ident . $name:ident) => {
        #[test]
        #[allow(non_snake_case)]
        fn $name() {
            let orc_data = include_bytes!(concat!("../examples/", stringify!($name_prefix), ".", stringify!($name), ".orc"));
            let jsn_gz_data: &[u8] = include_bytes!(concat!("../examples/expected/", stringify!($name_prefix), ".", stringify!($name), ".jsn.gz"));
            inner_test(orc_data, jsn_gz_data);
        }
    };
);

macro_rules! test_local(
    ($name:ident) => {
        #[test]
        #[allow(non_snake_case)]
        fn $name() {
            let orc_data = include_bytes!(concat!("../../", stringify!($name), ".orc"));
            let jsn_gz_data: &[u8] = include_bytes!(concat!("../../", stringify!($name), ".jsn.gz"));
            inner_test(orc_data, jsn_gz_data);
        }
    };
);

fn inner_test(orc_data: &[u8], jsn_gz_data: &[u8]) {
    let mut expected_jsn_bytes = Vec::new();
    let mut decoder = flate2::read::GzDecoder::new(jsn_gz_data);
    decoder
        .read_to_end(&mut expected_jsn_bytes)
        .expect("Could not gunzip .jsn.gz file");
    let expected_jsn =
        String::from_utf8(expected_jsn_bytes).expect("Invalid UTF-8 in .jsn.gz file");
    assert_eq!(orc_to_jsn(orc_data), expected_jsn);
}

fn unwrap_deserialization<T, E: Debug>(
    res: Result<(Vec<bool>, Vec<T>), E>,
    error_msg: &str,
) -> Vec<Option<T>> {
    let (validities, valid_values) = res.expect(error_msg);
    let mut valid_values_iter = valid_values.into_iter();
    let mut all_values = Vec::new();
    for validity in validities {
        all_values.push(
            if validity {
                Some(valid_values_iter.next().expect("Column too short"))
            }
            else {
                None
            }
        );
    }
    assert!(valid_values_iter.next().is_none());
    all_values
}

fn orc_to_jsn(orc_data: &[u8]) -> String {
    let mut orc_file = Cursor::new(orc_data);
    let metadata = read_metadata(&mut orc_file).expect("Failed to unwrap metadata");

    let stripe = 0;
    let stripe_footer = read_stripe_footer(&mut orc_file, &metadata, stripe, &mut vec![])
        .expect(&format!("Could not reater footer of stripe {}", stripe));

    let data = read_column(
        &mut orc_file,
        &metadata,
        stripe,
        &stripe_footer,
        0,
        0,
        "<root>",
    );

    let mut lines = data.iter()
        .map(JsonValue::dump)
        .collect::<Vec<_>>();
    lines.push(String::new());
    lines.join("\n")
}

fn read_column(
    orc_file: &mut Cursor<&[u8]>,
    metadata: &FileMetadata,
    stripe: usize,
    stripe_footer: &StripeFooter,
    type_id: u32,
    column_offset: u32,
    column_name: &str,
) -> Vec<JsonValue> {
    let type_ = metadata
        .footer
        .types
        .get(type_id as usize)
        .expect(&format!("missing type {}", type_id));

    match type_.kind() {
        Kind::Struct => {
            let mut columns = Vec::new();
            for (relative_column_index, &subtype_id) in type_.subtypes.iter().enumerate() {
                columns.push(read_column(
                    orc_file,
                    metadata,
                    stripe,
                    stripe_footer,
                    subtype_id,
                    (relative_column_index as u32) + column_offset + 1,
                    type_
                        .field_names
                        .get(type_id as usize)
                        .unwrap_or(&"<overflowed>".to_owned()),
                ))
            }

            let mut lines = Vec::new();
            'outer: loop {
                let mut row = Vec::new();
                for (column_index, column) in columns.iter_mut().enumerate() {
                    match column.pop() {
                        Some(cell) => row.push(cell),
                        None => {
                            if column_index == 0 {
                                break 'outer;
                            } else {
                                panic!(
                                    "Column {} is shorter than column {}",
                                    (column_index as u32) + column_offset,
                                    column_offset
                                );
                            }
                        }
                    }
                }
                lines.push(JsonValue::Array(row));
            }
            lines.reverse();
            lines
        }
        _ => {
            let column = read_stripe_column(
                orc_file,
                &metadata,
                stripe,
                stripe_footer.clone(),
                column_offset,
                vec![],
            )
            .expect(&format!(
                "Could not read stripe {} column {} ({})",
                stripe, column_offset, column_name,
            ));

            let error_msg = format!(
                "Could not deserialize stripe {} column {} ({}) as kind {:?}",
                stripe,
                column_offset,
                column_name,
                type_.kind()
            );
            match type_.kind() {
                Kind::Struct => unreachable!("Kind::Struct"),
                Kind::Boolean => {
                    unwrap_deserialization(deserialize_bool_array(&column), &error_msg)
                        .into_iter()
                        .map(|v| match v {
                            Some(b) => JsonValue::Boolean(b),
                            None => JsonValue::Null,
                        })
                        .collect()
                }
                Kind::Byte | Kind::Short | Kind::Int | Kind::Long => {
                    unwrap_deserialization(deserialize_int_array(&column), &error_msg)
                        .into_iter()
                        .map(|v| match v {
                            Some(n) => JsonValue::Number(Number::from(n)),
                            None => JsonValue::Null,
                        })
                        .collect()
                }
                Kind::Float => {
                    unwrap_deserialization(deserialize_f32_array(&column), &error_msg)
                        .into_iter()
                        .map(|v| match v {
                            Some(n) => JsonValue::Number(Number::from(n)),
                            None => JsonValue::Null,
                        })
                        .collect()
                }
                Kind::String => {
                    unwrap_deserialization(deserialize_str_array(&column), &error_msg)
                        .into_iter()
                        .map(|v| match v {
                            Some(n) => JsonValue::String(n),
                            None => JsonValue::Null,
                        })
                        .collect()
                }
                _ => panic!("{:?} not implemented", type_.kind()),
            }
        }
    }
}

test_apache!(TestOrcFile.metaData);
test_apache!(TestOrcFile.test1);
test_apache!(TestOrcFile.testSeek);
test_apache!(TestOrcFile.testTimestamp);
test_apache!(orc_file_11_format, "orc-file-11-format");

test_local!(long_bool);
test_local!(long_bool_gzip);
test_local!(string_long);
test_local!(string_dict);
test_local!(string_dict_gzip);
test_local!(string_long_long);
test_local!(string_long_long_gzip);

// These two fail because Rust's json uses exponent notation but Python's does not.
// test_local!(test);
// test_local!(f32_long_long_gzip);  
