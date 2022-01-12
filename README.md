# WIP to read Apache ORC from Rust

This repo contains minimal dependencies and generated code from proto to read Apache ORC.

Currently it only reads the file footer. Next to come.

## Run tests

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -U pyorc
venv/bin/python write.py
cargo test
```
