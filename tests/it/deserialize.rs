use orc_format::{
    fallible_streaming_iterator::FallibleStreamingIterator,
    proto::{column_encoding::Kind as ColumnEncodingKind, stream::Kind},
    read,
    read::decode::{
        BooleanIter, SignedRleV2Iter, SignedRleV2Run, UnsignedRleV2Iter, UnsignedRleV2Run,
    },
    read::decompress::Decompressor,
    read::Stripe,
    Error,
};

fn deserialize_validity(
    stripe: &Stripe,
    column: usize,
    scratch: &mut Vec<u8>,
) -> Result<Vec<bool>, Error> {
    let mut chunks = stripe.get_bytes(column, Kind::Present, std::mem::take(scratch))?;

    let mut validity = Vec::with_capacity(stripe.number_of_rows());
    let mut remaining = stripe.number_of_rows();
    while let Some(chunk) = chunks.next()? {
        let iter = BooleanIter::new(chunk, remaining);
        for item in iter {
            remaining -= 1;
            validity.push(item?)
        }
    }
    *scratch = std::mem::take(&mut chunks.into_inner());

    Ok(validity)
}

pub fn deserialize_f32_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<f32>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let mut chunks = stripe.get_bytes(column, Kind::Data, scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let mut valid_values = Vec::with_capacity(num_of_values);
    while let Some(chunk) = chunks.next()? {
        valid_values.extend(read::decode::deserialize_f32(chunk));
    }

    Ok((validity, valid_values))
}

pub fn deserialize_int_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<i64>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let mut chunks = stripe.get_bytes(column, Kind::Data, scratch)?;

    let mut valid_values = Vec::with_capacity(num_of_values);
    while let Some(chunk) = chunks.next()? {
        SignedRleV2Iter::new(chunk).try_for_each(|run| {
            run.map(|run| match run {
                SignedRleV2Run::Direct(values) => valid_values.extend(values),
                SignedRleV2Run::Delta(values) => valid_values.extend(values),
                SignedRleV2Run::ShortRepeat(values) => valid_values.extend(values),
            })
        })?;
    }

    Ok((validity, valid_values))
}

pub fn deserialize_bool_array(
    stripe: &Stripe,
    column: usize,
) -> Result<(Vec<bool>, Vec<bool>), Error> {
    let mut scratch = vec![];

    let validity = deserialize_validity(stripe, column, &mut scratch)?;

    let num_of_values: usize = validity.iter().map(|x| *x as usize).sum();

    let mut chunks = stripe.get_bytes(column, Kind::Data, std::mem::take(&mut scratch))?;

    let mut valid_values = Vec::with_capacity(num_of_values);
    let mut remaining = num_of_values;
    while let Some(chunk) = chunks.next()? {
        let iter = BooleanIter::new(chunk, remaining);
        for item in iter {
            remaining -= 1;
            valid_values.push(item?)
        }
    }

    Ok((validity, valid_values))
}

pub struct Values<'a> {
    values: Decompressor<'a>,
    offset: usize,
    is_first: bool,
}

impl<'a> Values<'a> {
    pub fn new(values: Decompressor<'a>) -> Self {
        Self {
            values,
            offset: 0,
            is_first: true,
        }
    }

    pub fn next(&mut self, length: usize) -> Result<String, Error> {
        if self.is_first {
            self.offset = 0;
            self.values.advance()?;
            self.is_first = false;
        }
        let current = self.values.get().ok_or(Error::OutOfSpec)?;
        let current = if self.offset == current.len() {
            self.offset = 0;
            self.values.advance()?;
            self.values.get().ok_or(Error::OutOfSpec)?
        } else {
            current
        };
        let item = current
            .get(self.offset..self.offset + length)
            .ok_or(Error::OutOfSpec);
        self.offset += length;
        item.and_then(|item| {
            std::str::from_utf8(item)
                .map(|x| x.to_string())
                .map_err(|_| Error::InvalidUtf8)
        })
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.values.into_inner()
    }
}

pub fn deserialize_str<'a>(
    values: &mut Values<'a>,
    mut lengths: Decompressor,
    num_of_values: usize,
) -> Result<Vec<String>, Error> {
    let mut result = Vec::with_capacity(num_of_values);
    while let Some(chunk) = lengths.next()? {
        for run in UnsignedRleV2Iter::new(chunk) {
            let f = |length| values.next(length as usize);
            match run? {
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
            }?
        }
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

    let mut values_iter = Values::new(values);

    let scratch2 = vec![];
    let lengths = stripe.get_bytes(column, Kind::Length, scratch2)?;

    let values = deserialize_str(&mut values_iter, lengths, 0)?;
    let scratch = values_iter.into_inner();

    let mut indices = stripe.get_bytes(column, Kind::Data, scratch)?;

    let f = |x| values.get(x as usize).cloned().ok_or(Error::OutOfSpec);

    let mut result = Vec::with_capacity(num_of_values);
    while let Some(chunk) = indices.next()? {
        read::decode::UnsignedRleV2Iter::new(chunk).try_for_each(|run| {
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
            })
        })?
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
    let mut values = Values::new(values);

    let scratch1 = vec![];
    let lengths = stripe.get_bytes(column, Kind::Length, scratch1)?;

    deserialize_str(&mut values, lengths, num_of_values)
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
