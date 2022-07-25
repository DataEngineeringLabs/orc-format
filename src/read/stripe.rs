use crate::{
    proto::{stream::Kind, ColumnEncoding, CompressionKind, StripeFooter, StripeInformation},
    Error,
};

use super::decompress::Decompressor;
use super::deserialize_stripe_footer;

#[derive(Debug)]
pub struct Stripe {
    stripe: Vec<u8>,
    information: StripeInformation,
    footer: StripeFooter,
    offsets: Vec<u64>,
    compression: CompressionKind,
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
            compression,
        })
    }

    pub fn get_bytes<'a>(
        &'a self,
        column: usize,
        kind: Kind,
        scratch: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], Error> {
        scratch.clear();
        let column = column as u32;
        self.footer
            .streams
            .iter()
            .zip(self.offsets.windows(2))
            .find(|(stream, _)| stream.column() == column && stream.kind() == kind)
            .map(|(stream, offsets)| {
                let start = offsets[0];
                debug_assert_eq!(offsets[1] - offsets[0], stream.length());
                let length = stream.length();
                let data = &self.stripe[start as usize..(start + length) as usize];
                super::decompress::maybe_decompress(data, self.compression, scratch)
            })
            .transpose()?
            .ok_or(Error::InvalidColumn(column, kind))
    }

    pub fn get_bytes_iter(
        &self,
        column: usize,
        kind: Kind,
        scratch: Vec<u8>,
    ) -> Result<Decompressor, Error> {
        let column = column as u32;
        self.footer
            .streams
            .iter()
            .zip(self.offsets.windows(2))
            .find(|(stream, _)| stream.column() == column && stream.kind() == kind)
            .map(|(stream, offsets)| {
                let start = offsets[0];
                debug_assert_eq!(offsets[1] - offsets[0], stream.length());
                let length = stream.length();
                let data = &self.stripe[start as usize..(start + length) as usize];
                Decompressor::new(data, self.compression, scratch)
            })
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
