use orc_format::{
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::decode::{
        BooleanIter, IteratorEnum, SignedRleV2Iter, SignedRleV2Run, UnsignedRleV2Iter,
        UnsignedRleV2Run,
    },
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

    let mut valid_values = Vec::with_capacity(num_of_rows);
    SignedRleV2Iter::new(data).try_for_each(|run| {
        run.map(|run| match run {
            SignedRleV2Run::Direct(values) => valid_values.extend(values),
            SignedRleV2Run::Delta(values) => valid_values.extend(values),
            SignedRleV2Run::ShortRepeat(values) => valid_values.extend(values),
        })
    })?;

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
) -> impl Iterator<
    Item = Result<
        IteratorEnum<
            impl Iterator<Item = Result<&'a str, Error>> + 'a,
            impl Iterator<Item = Result<&'a str, Error>> + 'a,
            impl Iterator<Item = Result<&'a str, Error>> + 'a,
        >,
        Error,
    >,
> {
    let f = move |length| {
        let (item, remaining) = values.split_at(length as usize);
        values = remaining;
        std::str::from_utf8(item).map_err(|_| Error::InvalidUtf8)
    };

    let decoder = read::decode::UnsignedRleV2Iter::new(lengths);
    decoder.map(move |run| {
        run.map(|run| match run {
            UnsignedRleV2Run::Direct(values) => IteratorEnum::Direct(values.map(f)),
            UnsignedRleV2Run::Delta(values) => IteratorEnum::Delta(values.map(f)),
            UnsignedRleV2Run::ShortRepeat(values) => IteratorEnum::ShortRepeat(values.map(f)),
        })
    })
}

pub fn deserialize_str_dict_array<'a>(
    stripe: &'a Stripe,
    column: usize,
    scratch: &'a mut Vec<u8>,
) -> Result<Vec<String>, Error> {
    let values = stripe.get_bytes(column, Kind::DictionaryData, scratch)?;
    let mut scratch2 = vec![];
    let lengths = stripe.get_bytes(column, Kind::Length, &mut scratch2)?;

    let mut result = Vec::with_capacity(stripe.number_of_rows());
    deserialize_str(values, lengths).try_for_each(|run| {
        match run? {
            IteratorEnum::Direct(mut values) => {
                values.try_for_each(|x| x.map(|x| result.push(x)))?
            }
            IteratorEnum::Delta(mut values) => {
                values.try_for_each(|x| x.map(|x| result.push(x)))?
            }
            IteratorEnum::ShortRepeat(mut values) => {
                values.try_for_each(|x| x.map(|x| result.push(x)))?
            }
        };
        Result::<_, Error>::Ok(())
    })?;

    let mut scratch3 = vec![];
    let indices = stripe.get_bytes(column, Kind::Data, &mut scratch3)?;

    let f = |x| result[x as usize].to_string();

    let mut valid_values = Vec::with_capacity(stripe.number_of_rows());
    for run in UnsignedRleV2Iter::new(indices) {
        match run? {
            UnsignedRleV2Run::Direct(values) => valid_values.extend(values.map(f)),
            UnsignedRleV2Run::Delta(values) => valid_values.extend(values.map(f)),
            UnsignedRleV2Run::ShortRepeat(values) => valid_values.extend(values.map(f)),
        };
    }

    Ok(valid_values)
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

            let mut result = Vec::with_capacity(stripe.number_of_rows());
            deserialize_str(values, lengths).try_for_each(|run| {
                match run? {
                    IteratorEnum::Direct(mut values) => values.try_for_each(|x| {
                        result.push(x?.to_string());
                        Result::<_, Error>::Ok(())
                    })?,
                    IteratorEnum::Delta(mut values) => values.try_for_each(|x| {
                        result.push(x?.to_string());
                        Result::<_, Error>::Ok(())
                    })?,
                    IteratorEnum::ShortRepeat(mut values) => values.try_for_each(|x| {
                        result.push(x?.to_string());
                        Result::<_, Error>::Ok(())
                    })?,
                };
                Result::<_, Error>::Ok(())
            })?;
            result
        }

        ColumnEncodingKind::DictionaryV2 => {
            deserialize_str_dict_array(stripe, column, &mut scratch)?
        }
        other => todo!("{other:?}"),
    };
    Ok((validity, valid_values))
}
