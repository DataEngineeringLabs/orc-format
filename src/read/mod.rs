//! APIs to read from ORC
//!
//! Reading from ORC is essentially composed by:
//! 1. Identify the column type based on the file's schema
//! 2. Read the stripe (or part of it in projection pushdown)
//! 3. For each column, select the relevant region of the stripe
//! 4. Attach an Iterator to the region

use std::io::{Read, Seek, SeekFrom};

use prost::Message;

use crate::error::Error;
use crate::proto::stream::Kind;
use crate::proto::{CompressionKind, Footer, Metadata, PostScript, StripeFooter};

mod column;
pub mod decode;
pub mod decompress;
pub use column::Column;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.seek(SeekFrom::Current(0))?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

/// The file's metadata.
#[derive(Debug)]
pub struct FileMetadata {
    pub postscript: PostScript,
    pub footer: Footer,
    pub metadata: Metadata,
}

pub fn read_metadata<R>(reader: &mut R) -> Result<FileMetadata, Error>
where
    R: Read + Seek,
{
    let file_len = stream_len(reader)?;

    // initial read of the footer
    let footer_len = if file_len < DEFAULT_FOOTER_SIZE {
        file_len
    } else {
        DEFAULT_FOOTER_SIZE
    };

    reader.seek(SeekFrom::End(-(footer_len as i64)))?;
    let mut tail_bytes = Vec::with_capacity(footer_len as usize);
    reader.take(footer_len).read_to_end(&mut tail_bytes)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as usize;
    tail_bytes.truncate(tail_bytes.len() - 1);

    // next is the postscript
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len..])?;
    tail_bytes.truncate(tail_bytes.len() - postscript_len);

    // next is the footer
    let footer_length = postscript.footer_length.ok_or(Error::OutOfSpec)? as usize; // todo: throw error

    let footer = &tail_bytes[tail_bytes.len() - footer_length..];
    let footer = deserialize_footer(footer, postscript.compression())?;
    tail_bytes.truncate(tail_bytes.len() - footer_length);

    // finally the metadata
    let metadata_length = postscript.metadata_length.ok_or(Error::OutOfSpec)? as usize; // todo: throw error
    let metadata = &tail_bytes[tail_bytes.len() - metadata_length..];
    let metadata = deserialize_footer_metadata(metadata, postscript.compression())?;

    Ok(FileMetadata {
        postscript,
        footer,
        metadata,
    })
}

/// Reads, decompresses and deserializes the stripe's footer as [`StripeFooter`] using
/// `scratch` as an intermediary memory region.
/// # Implementation
/// This function is guaranteed to perform exactly one seek and one read to `reader`.
pub fn read_stripe_footer<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    stripe: usize,
    scratch: &mut Vec<u8>,
) -> Result<StripeFooter, Error> {
    let stripe = &metadata.footer.stripes[stripe];

    let start = stripe.offset() + stripe.index_length() + stripe.data_length();
    let len = stripe.footer_length();
    reader.seek(SeekFrom::Start(start))?;

    scratch.clear();
    scratch.reserve(len as usize);
    reader.take(len).read_to_end(scratch)?;
    deserialize_stripe_footer(scratch, metadata.postscript.compression())
}

/// Reads `column` from the stripe into a [`Column`].
/// `scratch` becomes owned by [`Column`], which you can recover via `into_inner`.
/// # Implementation
/// This function is guaranteed to perform exactly one seek and one read to `reader`.
pub fn read_stripe_column<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    stripe: usize,
    footer: StripeFooter,
    column: u32,
    mut scratch: Vec<u8>,
) -> Result<Column, Error> {
    let stripe = &metadata.footer.stripes[stripe];

    let mut start = 0; // the start of the stream

    let start = footer
        .streams
        .iter()
        .map(|stream| {
            start += stream.length();
            (start, stream)
        })
        .find(|(_, stream)| stream.column() == column && stream.kind() != Kind::RowIndex)
        .map(|(start, stream)| start - stream.length())
        .ok_or(Error::InvalidColumn(column))?;

    let length = footer
        .streams
        .iter()
        .filter(|stream| stream.column() == column && stream.kind() != Kind::RowIndex)
        .fold(0, |acc, stream| acc + stream.length());

    let start = stripe.offset() + start;
    reader.seek(SeekFrom::Start(start))?;

    scratch.clear();
    scratch.reserve(length as usize);
    reader.take(length).read_to_end(&mut scratch)?;
    Ok(Column::new(
        scratch,
        column,
        stripe.number_of_rows(),
        footer,
        metadata.postscript.compression(),
    ))
}

fn deserialize_footer(bytes: &[u8], compression: CompressionKind) -> Result<Footer, Error> {
    let mut buffer = vec![];
    decompress::Decompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(Footer::decode(&*buffer)?)
}

fn deserialize_footer_metadata(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<Metadata, Error> {
    let mut buffer = vec![];
    decompress::Decompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(Metadata::decode(&*buffer)?)
}

fn deserialize_stripe_footer(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<StripeFooter, Error> {
    let mut buffer = vec![];
    decompress::Decompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(StripeFooter::decode(&*buffer)?)
}
