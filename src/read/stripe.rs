use crate::{
    error::Error,
    proto::{stream::Kind, ColumnEncoding, CompressionKind, StripeFooter},
};

use super::decompress::Decompressor;

/// Helper struct used to access the streams associated to an ORC column.
/// Its main use [`Column::get_stream`], to get a stream.
#[derive(Debug)]
pub struct Column {
    data: Vec<u8>,
    column: u32,
    number_of_rows: u64,
    footer: StripeFooter,
    compression: CompressionKind,
}

impl Column {
    pub(crate) fn new(
        data: Vec<u8>,
        column: u32,
        number_of_rows: u64,
        footer: StripeFooter,
        compression: CompressionKind,
    ) -> Self {
        Self {
            data,
            column,
            number_of_rows,
            footer,
            compression,
        }
    }

    /// Returns the stream `kind` associated to this column as a [`Decompressor`].
    /// `scratch` becomes owned by [`Decompressor`], which you can recover via `into_inner`.
    pub fn get_stream(&self, kind: Kind, scratch: Vec<u8>) -> Result<Decompressor, Error> {
        let mut start = 0; // the start of the stream
        self.footer
            .streams
            .iter()
            .filter(|stream| stream.column() == self.column && stream.kind() != Kind::RowIndex)
            .map(|stream| {
                start += stream.length() as usize;
                stream
            })
            .find(|stream| stream.kind() == kind)
            .map(|stream| {
                let length = stream.length() as usize;
                let data = &self.data[start - length..start];
                Decompressor::new(data, self.compression, scratch)
            })
            .ok_or(Error::InvalidKind(self.column, kind))
    }

    pub fn encoding(&self) -> &ColumnEncoding {
        &self.footer.columns[self.column as usize]
    }

    pub fn dictionary_size(&self) -> Option<usize> {
        self.footer.columns[self.column as usize]
            .dictionary_size
            .map(|x| x as usize)
    }

    /// The number of rows on this column
    pub fn number_of_rows(&self) -> usize {
        self.number_of_rows as usize
    }

    /// Returns the underlying scratch containing a pre-allocated memory region
    /// containing all (compressed) streams of this column.
    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }
}
