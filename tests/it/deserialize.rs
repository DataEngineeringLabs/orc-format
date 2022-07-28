use orc_format::{
    error::Error,
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::decode::{
        BooleanIter, SignedRleV2Iter, SignedRleV2Run, UnsignedRleV2Iter, UnsignedRleV2Run,
    },
    read::decompress::StreamingDecompressor,
    read::Stripe,
};

fn deserialize_validity(
    stripe: &Stripe,
    column: usize,
    scratch: &mut Vec<u8>,
) -> Result<Vec<bool>, Error> {
    let mut reader = stripe.get_bytes(column, Kind::Present, std::mem::take(scratch))?;

    let mut validity = Vec::with_capacity(stripe.number_of_rows());
    BooleanIter::new(&mut reader, stripe.number_of_rows()).try_for_each(|item| {
        validity.push(item?);
        Result::<(), Error>::Ok(())
    })?;

    *scratch = std::mem::take(&mut reader.into_inner());

    Ok(validity)
}

pub fn deserialize_f32_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<f32>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut reader = stripe.get_bytes(column, Kind::Data, scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let mut valid_values = Vec::with_capacity(num_of_values);
    read::decode::FloatIter::<f32, _>::new(&mut reader, num_of_values).try_for_each(|item| {
        valid_values.push(item?);
        Result::<(), Error>::Ok(())
    })?;

    Ok((validity, valid_values))
}

pub fn deserialize_int_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<i64>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let reader = stripe.get_bytes(column, Kind::Data, scratch)?;

    let mut valid_values = Vec::with_capacity(num_of_values);

    SignedRleV2Iter::new(reader, num_of_values, vec![]).try_for_each(|run| {
        run.map(|run| match run {
            SignedRleV2Run::Direct(values) => valid_values.extend(values),
            SignedRleV2Run::Delta(values) => valid_values.extend(values),
            SignedRleV2Run::ShortRepeat(values) => valid_values.extend(values),
        })
    })?;

    Ok((validity, valid_values))
}

pub fn deserialize_bool_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let mut reader = stripe.get_bytes(column, Kind::Data, std::mem::take(&mut scratch))?;

    let mut valid_values = Vec::with_capacity(num_of_values);
    BooleanIter::new(&mut reader, num_of_values).try_for_each(|item| {
        valid_values.push(item?);
        Result::<(), Error>::Ok(())
    })?;

    Ok((validity, valid_values))
}

pub fn deserialize_str(
    lengths: UnsignedRleV2Iter<StreamingDecompressor>,
    values: &mut read::decode::Values<StreamingDecompressor>,
    num_of_values: usize,
) -> Result<Vec<String>, Error> {
    let mut result = Vec::with_capacity(num_of_values);

    for run in lengths {
        let f = |length| {
            values.next(length as usize).and_then(|x| {
                std::str::from_utf8(x)
                    .map(|x| x.to_string())
                    .map_err(|_| Error::InvalidUtf8)
            })
        };
        match run? {
            UnsignedRleV2Run::Direct(lengths) => lengths.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
            UnsignedRleV2Run::Delta(lengths) => lengths.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
            UnsignedRleV2Run::ShortRepeat(lengths) => lengths.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
        }?
    }
    Ok(result)
}

pub fn deserialize_str_dict_array(
    stripe: &Stripe,
    column: usize,
    scratch: Vec<u8>,
    num_of_values: usize,
) -> Result<Vec<String>, Error> {
    let values = stripe.get_bytes(column, Kind::DictionaryData, scratch)?;

    let mut values_iter = read::decode::Values::new(values, vec![]);

    let scratch2 = vec![];
    let mut lengths = stripe.get_bytes(column, Kind::Length, scratch2)?;

    let lengths = UnsignedRleV2Iter::new(
        &mut lengths,
        stripe.columns()[column].dictionary_size() as usize,
        vec![],
    );

    let values = deserialize_str(lengths, &mut values_iter, 0)?;
    let scratch = values_iter.into_inner();

    let mut indices = stripe.get_bytes(column, Kind::Data, scratch)?;
    let indices = UnsignedRleV2Iter::new(&mut indices, stripe.number_of_rows(), vec![]);

    let f = |x| values.get(x as usize).cloned().ok_or(Error::OutOfSpec);

    let mut result = Vec::with_capacity(num_of_values);
    for run in indices {
        run.and_then(|run| match run {
            UnsignedRleV2Run::Direct(values) => values.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
            UnsignedRleV2Run::Delta(values) => values.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
            UnsignedRleV2Run::ShortRepeat(values) => values.map(f).try_for_each(|x| {
                result.push(x?);
                Result::<_, Error>::Ok(())
            }),
        })?;
    }

    Ok(result)
}

fn deserialize_str_array_direct(
    stripe: &Stripe,
    column: usize,
    scratch: Vec<u8>,
    num_of_values: usize,
) -> Result<Vec<String>, Error> {
    let values = stripe.get_bytes(column, Kind::Data, scratch)?;
    let mut values = read::decode::Values::new(values, vec![]);

    let scratch1 = vec![];
    let mut lengths = stripe.get_bytes(column, Kind::Length, scratch1)?;
    let lengths = UnsignedRleV2Iter::new(&mut lengths, num_of_values, vec![]);

    deserialize_str(lengths, &mut values, num_of_values)
}

pub fn deserialize_str_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<String>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    // todo: generalize to other encodings
    let encoding = stripe.get_encoding(column)?;
    let valid_values = match encoding.kind() {
        ColumnEncodingKind::DirectV2 => {
            deserialize_str_array_direct(stripe, column, scratch, num_of_values)?
        }
        ColumnEncodingKind::DictionaryV2 => {
            deserialize_str_dict_array(stripe, column, scratch, num_of_values)?
        }
        other => todo!("{other:?}"),
    };
    Ok((validity, valid_values))
}
