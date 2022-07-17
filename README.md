# WIP to read Apache ORC from Rust

[![test](https://github.com/jorgecarleitao/orc-rs/actions/workflows/test.yml/badge.svg)](https://github.com/jorgecarleitao/orc-rs/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/jorgecarleitao/orc-rs/branch/main/graph/badge.svg?token=AgyTF60R3D)](https://codecov.io/gh/jorgecarleitao/orc-rs)

This repo contains minimal dependencies and generated code from proto to read Apache ORC.

It currently reads:

* metadata (proto files)
* stripes
* booleans
* strings (non-dictionary encoded)
* integers (non-dictionary encoded)
* floats (non-dictionary encoded)

## Run tests

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -U pyorc
venv/bin/python write.py
cargo test
```
