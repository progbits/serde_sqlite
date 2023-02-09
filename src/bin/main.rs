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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        print!("usage: desql DATABASE_FILE");
        std::process::exit(1);
    }

    // Open the SQLite database file and read the whole file.
    let path = std::path::Path::new(&args[1]);
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
