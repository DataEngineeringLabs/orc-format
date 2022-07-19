#![forbid(unsafe_code)]

use std::io::{Read, Seek, SeekFrom};

use prost::Message;

use crate::proto::{CompressionKind, Footer, Metadata, PostScript, StripeFooter};

use super::Error;

pub mod decode;
mod decompress;
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
    Ok(Footer::decode(decompress::maybe_decompress(
        bytes,
        compression,
        &mut vec![],
    )?)?)
}

fn deserialize_footer_metadata(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<Metadata, Error> {
    Ok(Metadata::decode(decompress::maybe_decompress(
        bytes,
        compression,
        &mut vec![],
    )?)?)
}

fn deserialize_stripe_footer(
    bytes: &[u8],
    compression: CompressionKind,
) -> Result<StripeFooter, Error> {
    Ok(StripeFooter::decode(decompress::maybe_decompress(
        bytes,
        compression,
        &mut vec![],
    )?)?)
}
