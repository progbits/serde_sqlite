# serde_sqlite

[![Build](https://github.com/progbits/serde_sqlite/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/progbits/serde_sqlite/actions/workflows/build.yaml)

A Serde deserializer for SQLite database files.

## Getting Started

## Running the tests

Integration tests can be run using cargo 

```
$ cargo test
```

Because some integration tests rely on randomly generated data, it can be useful to run these tests repeatedly to catch
edge cases.

```
$ ./scripts/fuzz.sh
```

