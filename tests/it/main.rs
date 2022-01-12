use std::fs::File;

use orc_format::read;

#[test]
fn read_schema() {
    let mut f = File::open(&"test.orc").expect("no file found");

    let (ps, footer, metadata) = read::read_metadata(&mut f).unwrap();
}
