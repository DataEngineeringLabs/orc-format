use crate::{
    proto::{stream::Kind, ColumnEncoding, CompressionKind, StripeFooter, StripeInformation},
    Error,
};

use super::deserialize_stripe_footer;

#[derive(Debug)]
pub struct Stripe {
    stripe: Vec<u8>,
    information: StripeInformation,
    footer: StripeFooter,
    offsets: Vec<u64>,
}

impl Stripe {
    pub fn try_new(
        stripe: Vec<u8>,
        information: StripeInformation,
        compression: CompressionKind,
    ) -> Result<Self, Error> {
        let footer_offset = (information.index_length() + information.data_length()) as usize;
        let footer = deserialize_stripe_footer(&stripe[footer_offset..], compression)?;

        let offsets: Vec<u64> = footer.streams.iter().fold(vec![0], |mut acc, v| {
            acc.push(acc.last().copied().unwrap() + v.length());
            acc
        });

        Ok(Self {
            stripe,
            information,
            footer,
            offsets,
        })
    }

    pub fn get_bytes(&self, column: usize, kind: Kind) -> Result<&[u8], Error> {
        let column = column as u32;
        self.footer
            .streams
            .iter()
            .zip(self.offsets.windows(2))
            .filter(|(stream, _)| stream.column() == column && stream.kind() == kind)
            .map(|(stream, offsets)| {
                let start = offsets[0];
                debug_assert_eq!(offsets[1] - offsets[0], stream.length());
                let length = stream.length();
                &self.stripe[start as usize..(start + length) as usize]
            })
            .next()
            .ok_or(Error::InvalidColumn(column, kind))
    }

    pub fn get_encoding(&self, column: usize) -> Result<&ColumnEncoding, Error> {
        self.footer.columns.get(column).ok_or(Error::OutOfSpec)
    }

    pub fn number_of_rows(&self) -> usize {
        self.information.number_of_rows() as usize
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.stripe
    }
}
