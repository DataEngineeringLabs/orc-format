Welcome to `orc-format` documentation. Thanks for checking it out!

This Rust crate is a toolkit to read and deserialize ORC to your favourite in-memory format.

Below is an example of how to read a column from ORC into memory:

```rust
use std::fs::File;

use orc_format::{error::Error, read, read::Column};


fn get_column(path: &str, column: u32) -> Result<Column, Error> {
    // open the file, as expected. buffering this is not necessary - we
    // are very careful about the number of `read`s we perform.
    let mut f = File::open(path).expect("no file found");

    // read the files' metadata
    let metadata = read::read_metadata(&mut f)?;

    // the next step is to identify which stripe we want to read. Let's say it is the first one.
    let stripe = 0;

    // Each stripe has a footer - we need to read it to extract the location of each column on it.
    let stripe_footer = read::read_stripe_footer(&mut f, &metadata, stripe, &mut vec![])?;

    // Finally, we read the column into `Column`
    read::read_stripe_column(&mut f, &metadata, stripe, stripe_footer, column, vec![])
}
```

To deserialize the values of a column, use things inside `read::decode`.
For example, the below contains the deserialization of the "Present" to a `Vec<bool>`.

```rust
use orc_format::{error::Error, proto::stream::Kind, read::decode::BooleanIter, read::Column};

fn deserialize_present(column: &Column, scratch: &mut Vec<u8>) -> Result<Vec<bool>, Error> {
    let mut reader = column.get_stream(Kind::Present, std::mem::take(scratch))?;

    let mut validity = Vec::with_capacity(column.number_of_rows());
    BooleanIter::new(&mut reader, column.number_of_rows()).try_for_each(|item| {
        validity.push(item?);
        Result::<(), Error>::Ok(())
    })?;

    *scratch = std::mem::take(&mut reader.into_inner());

    Ok(validity)
}
```

Check out the integration tests of the crate to find deserialization of other types such
as floats, integers, strings and dictionaries.
