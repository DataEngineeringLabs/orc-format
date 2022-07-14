use std::io::{Read, Seek, SeekFrom};

use prost::Message;

use crate::proto::{Footer, Metadata, PostScript, StripeFooter};

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

pub fn read_metadata<R>(reader: &mut R) -> Result<(PostScript, Footer, Metadata), std::io::Error>
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
    let footer = deserialize_footer(footer)?;
    tail_bytes.truncate(tail_bytes.len() - footer_length);

    // finally the metadata
    let metadata_length = postscript.metadata_length.unwrap() as usize; // todo: throw error
    let metadata = &tail_bytes[tail_bytes.len() - metadata_length..];
    let metadata = deserialize_footer_metadata(metadata)?;

    Ok((postscript, footer, metadata))
}

macro_rules! deserialize {
    ($bytes:expr, $op:ident) => {{
        let bytes = $bytes;
        let mut a: [u8; 4] = (&bytes[..4]).try_into().unwrap();
        a[3] = 0;
        let a = u32::from_le_bytes(a);
        let is_original = a % 2 == 1;
        let _length = (a / 2) as usize;

        if is_original {
            Ok($op(&bytes[3..])?)
        } else {
            let mut gz = flate2::read::DeflateDecoder::new(&bytes[3..]);
            let mut bytes = Vec::<u8>::new();
            gz.read_to_end(&mut bytes).unwrap();
            Ok($op(&bytes)?)
        }
    }};
}

fn deserialize_footer(footer: &[u8]) -> Result<Footer, std::io::Error> {
    let f = Footer::decode;
    deserialize!(footer, f)
}

fn deserialize_footer_metadata(bytes: &[u8]) -> Result<Metadata, std::io::Error> {
    let f = Metadata::decode;
    deserialize!(bytes, f)
}

pub fn deserialize_stripe_footer(bytes: &[u8]) -> Result<StripeFooter, std::io::Error> {
    let f = StripeFooter::decode;
    deserialize!(bytes, f)
}
