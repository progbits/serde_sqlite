# serde_sqlite

[![Build](https://github.com/progbits/serde_sqlite/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/progbits/serde_sqlite/actions/workflows/build.yaml)

A Serde deserializer for SQLite database files.

## Getting Started

Given the following table:

```sql
CREATE TABLE test_table(
	id INT, 
	float_column REAL, 
	text_column STRING, 
	blob_column BLOB
);
```

The table can be deserialized directly from the database file to the
`TestTable` struct:

```rust
use serde::Deserialize;
use std::io::Read;

use serde_sqlite::from_bytes;

#[derive(Deserialize, Debug)]
#[serde(rename(deserialize = "test_table"))]
struct TestTable {
    id: i64,
    float_column: f64,
    text_column: String,
    blob_column: Vec<u8>,
}

fn dump_table(db_file: &str) {
    // Read the SQLite database file.
    let path = std::path::Path::new(db_file);
    let bytes = match std::fs::File::open(path) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes
        }
        Err(err) => panic!("failed to open file: {:?}", err),
    };

    // Parse the `test_table` table.
    let iter = from_bytes::<TestTable>(&bytes).unwrap();
    for row in iter {
        println!("{:?}", row.unwrap());
    }
}
```

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

