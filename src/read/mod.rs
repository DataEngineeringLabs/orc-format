//! APIs to read from ORC
//!
//! Reading from ORC is essentially composed by:
//! 1. Identify the column type based on the file's schema
//! 2. Read the stripe (or part of it in projection pushdown)
//! 3. For each column, select the relevant region of the stripe
//! 4. Attach an Iterator to the region
#![forbid(unsafe_code)]

use std::io::{Read, Seek, SeekFrom};

use prost::Message;

use crate::error::Error;
use crate::proto::{CompressionKind, Footer, Metadata, PostScript, StripeFooter};

pub mod decode;
pub mod decompress;
mod stripe;
pub use stripe::Stripe;

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

pub fn read_metadata<R>(reader: &mut R) -> Result<(PostScript, Footer, Metadata), Error>
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
    let mut tail_bytes = vec![0; footer_len as usize];
    reader.read_exact(&mut tail_bytes)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as usize;
    tail_bytes.truncate(tail_bytes.len() - 1);

    // next is the postscript
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len..])?;
    tail_bytes.truncate(tail_bytes.len() - postscript_len);

    // next is the footer
    let footer_length = postscript.footer_length.unwrap() as usize; // todo: throw error

    let footer = &tail_bytes[tail_bytes.len() - footer_length..];
    let footer = deserialize_footer(footer, postscript.compression())?;
    tail_bytes.truncate(tail_bytes.len() - footer_length);

    // finally the metadata
    let metadata_length = postscript.metadata_length.unwrap() as usize; // todo: throw error
    let metadata = &tail_bytes[tail_bytes.len() - metadata_length..];
    let metadata = deserialize_footer_metadata(metadata, postscript.compression())?;

    Ok((postscript, footer, metadata))
}

fn deserialize_footer(bytes: &[u8], compression: CompressionKind) -> Result<Footer, Error> {
    let mut buffer = vec![];
    decompress::StreamingDecompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(Footer::decode(&*buffer)?)
}

fn deserialize_footer_metadata(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<Metadata, Error> {
    let mut buffer = vec![];
    decompress::StreamingDecompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(Metadata::decode(&*buffer)?)
}

fn deserialize_stripe_footer(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<StripeFooter, Error> {
    let mut buffer = vec![];
    decompress::StreamingDecompressor::new(bytes, compression, vec![]).read_to_end(&mut buffer)?;
    Ok(StripeFooter::decode(&*buffer)?)
}
