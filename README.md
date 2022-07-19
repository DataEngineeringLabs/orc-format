# Read Apache ORC from Rust

[![test](https://github.com/DataEngineeringLabs/orc-format/actions/workflows/test.yml/badge.svg)](https://github.com/DataEngineeringLabs/orc-format/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/DataEngineeringLabs/orc-format/branch/main/graph/badge.svg?token=AgyTF60R3D)](https://codecov.io/gh/DataEngineeringLabs/orc-format)

Read [Apache ORC](https://orc.apache.org/) in Rust.

This repository is similar to [parquet2](https://github.com/jorgecarleitao/parquet2) and [Avro-schema](https://github.com/DataEngineeringLabs/avro-schema), providing a toolkit to:

* Read ORC files (proto structures)
* Read stripes (the conversion from proto metadata to memory regions)
* Decode stripes (the math of decode stripes into e.g. booleans, runs of RLE, etc.)

It currently reads the following (logical) types:

* booleans
* strings
* integers
* floats

What is not yet implemented:

* Snappy, LZO decompression
* streaming decompression (i.e. if a stripe is compressed in chunks - we currently only
  support a single chunk)
* RLE v2 `Patched Base` decoding
* RLE v1 decoding
* Utility functions to decode non-native logical types:
    * decimal
    * timestamp
    * struct
    * List
    * Union

## Run tests

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -U pyorc
venv/bin/python write.py
cargo test
```
