use orc_format::{
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::decode::{BooleanIter, IteratorEnum},
    read::Stripe,
    Error,
};

pub fn deserialize_f32_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<f32>), Error> {
    let mut scratch = vec![];

    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present, &mut scratch)?;

    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let data = stripe.get_bytes(column, Kind::Data, &mut scratch)?;

    let valid_values = read::decode::deserialize_f32(data).collect::<Vec<_>>();
    Ok((validity, valid_values))
}

pub fn deserialize_int_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<i64>), Error> {
    let mut scratch = vec![];

    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present, &mut scratch)?;

    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let data = stripe.get_bytes(column, Kind::Data, &mut scratch)?;

    let valid_values = match read::decode::v2_signed(data)? {
        IteratorEnum::Direct(values) => values.collect::<Vec<_>>(),
        IteratorEnum::Delta(values) => values.collect::<Vec<_>>(),
        IteratorEnum::ShortRepeat(values) => values.collect::<Vec<_>>(),
    };

    Ok((validity, valid_values))
}

pub fn deserialize_bool(
    stream: &[u8],
    num_of_rows: usize,
) -> impl Iterator<Item = Result<bool, Error>> + '_ {
    BooleanIter::new(stream, num_of_rows)
}

pub fn deserialize_bool_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let mut scratch = vec![];

    let num_of_rows = stripe.number_of_rows();
    let data = stripe.get_bytes(column, Kind::Present, &mut scratch)?;

    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    let num_valids = validity.iter().filter(|x| **x).count();

    let data = stripe.get_bytes(column, Kind::Data, &mut scratch)?;

    let valid_values = deserialize_bool(data, num_valids).collect::<Result<Vec<_>, Error>>()?;
    Ok((validity, valid_values))
}

pub fn deserialize_str<'a>(
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

pub fn deserialize_str_dict_array<'a>(
    stripe: &'a Stripe,
    column: usize,
    scratch: &'a mut Vec<u8>,
) -> Result<
    IteratorEnum<
        impl Iterator<Item = String> + 'a,
        impl Iterator<Item = String> + 'a,
        impl Iterator<Item = String> + 'a,
    >,
    Error,
> {
    let values = stripe.get_bytes(column, Kind::DictionaryData, scratch)?;
    let mut scratch2 = vec![];
    let lengths = stripe.get_bytes(column, Kind::Length, &mut scratch2)?;

    let values = match deserialize_str(values, lengths)? {
        IteratorEnum::Direct(values) => values
            .map(|x| x.map(|x| x.to_string()))
            .collect::<Result<Vec<_>, Error>>()?,
        IteratorEnum::Delta(values) => values
            .map(|x| x.map(|x| x.to_string()))
            .collect::<Result<Vec<_>, Error>>()?,
        IteratorEnum::ShortRepeat(values) => values
            .map(|x| x.map(|x| x.to_string()))
            .collect::<Result<Vec<_>, Error>>()?,
    };

    let indices = stripe.get_bytes(column, Kind::Data, scratch)?;

    let f = move |x| values[x as usize].clone();

    Ok(match read::decode::v2_unsigned(indices)? {
        IteratorEnum::Direct(values) => IteratorEnum::Direct(values.map(f)),
        IteratorEnum::Delta(values) => IteratorEnum::Delta(values.map(f)),
        IteratorEnum::ShortRepeat(values) => IteratorEnum::ShortRepeat(values.map(f)),
    })
}

pub fn deserialize_str_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<String>), Error> {
    let num_of_rows = stripe.number_of_rows();

    let mut scratch = vec![];

    let data = stripe.get_bytes(column, Kind::Present, &mut scratch)?;
    let iter = BooleanIter::new(data, num_of_rows);
    let validity = iter.collect::<Result<Vec<_>, Error>>()?;

    // todo: generalize to other encodings
    let encoding = stripe.get_encoding(column)?;
    let valid_values = match encoding.kind() {
        ColumnEncodingKind::DirectV2 => {
            let values = stripe.get_bytes(column, Kind::Data, &mut scratch)?;

            let mut scratch1 = vec![];
            let lengths = stripe.get_bytes(column, Kind::Length, &mut scratch1)?;

            let x = match deserialize_str(values, lengths)? {
                IteratorEnum::Direct(values) => values
                    .map(|x| x.map(|x| x.to_string()))
                    .collect::<Result<Vec<_>, Error>>()?,
                IteratorEnum::Delta(values) => values
                    .map(|x| x.map(|x| x.to_string()))
                    .collect::<Result<Vec<_>, Error>>()?,
                IteratorEnum::ShortRepeat(values) => values
                    .map(|x| x.map(|x| x.to_string()))
                    .collect::<Result<Vec<_>, Error>>()?,
            };
            x
        }

        ColumnEncodingKind::DictionaryV2 => {
            match deserialize_str_dict_array(stripe, column, &mut scratch)? {
                IteratorEnum::Direct(values) => values.collect::<Vec<_>>(),
                IteratorEnum::Delta(values) => values.collect::<Vec<_>>(),
                IteratorEnum::ShortRepeat(values) => values.collect::<Vec<_>>(),
            }
        }
        other => todo!("{other:?}"),
    };
    Ok((validity, valid_values))
}
