use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::{thread_rng, Rng, RngCore};
use std::iter;

use rusqlite::{params, Connection};
use serde::Deserialize;
use std::io::Read;
use tempfile::NamedTempFile;

use serde_sqlite::de::*;

/// Alphabet sampled to generate random strings.
const ALPHABET: [char; 26] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z',
];

const MAX_COLUMN_SIZE: usize = 16;

const CREATE_TABLE_STMT: &str = "CREATE TABLE example_table (a TEXT, b INT, c REAL, d BLOB)";

const INSERT_STMT: &str = "INSERT INTO example_table VALUES (?1, ?2, ?3, ?4)";

/// An example database record with optional fields.
#[derive(Deserialize, PartialEq, Debug)]
#[serde(rename(deserialize = "example_table"))]
struct TestRecordOpt {
    a: Option<String>,
    b: Option<i64>,
    c: Option<f64>,
    d: Option<Vec<u8>>,
}

/// An example database record.
#[derive(Deserialize, PartialEq, Debug)]
#[serde(rename(deserialize = "example_table"))]
struct TestRecord {
    a: String,
    b: i64,
    c: f64,
    d: Vec<u8>,
}

/// SQLite type: TEXT.
/// The value is a text string, stored using the database encoding (UTF-8,
/// UTF-16BE or UTF-16LE).
fn random_text() -> String {
    let mut rng = thread_rng();
    let size = rng.gen::<usize>() % MAX_COLUMN_SIZE;
    iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(size)
        .collect()
}

/// SQLite type: INTEGER.
/// The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes
/// depending on the magnitude of the value.
fn random_integer() -> i64 {
    let mut rng = thread_rng();
    let between = Uniform::from(i64::MIN..i64::MAX);
    between.sample(&mut rng) as i64
}

/// SQLite type: REAL.
/// The value is a floating point value, stored as an 8-byte IEEE floating
/// point number.
fn random_real() -> f64 {
    let mut rng = thread_rng();
    rng.gen::<f64>()
}

/// SQLite type: BLOB.
/// The value is a blob of data, stored exactly as it was input.
fn random_blob() -> Vec<u8> {
    let mut rng = thread_rng();
    (0..rng.gen::<usize>() % MAX_COLUMN_SIZE)
        .map(|_| rand::random::<u8>())
        .collect()
}

/// Generate a random string.
fn random_string() -> String {
    let mut rng = thread_rng();
    let mut name = String::new();

    let length = rng.gen::<usize>() % 13 + 3;
    for _ in 0..length {
        let random_char_index = rng.gen::<usize>() % ALPHABET.len();
        name.push(ALPHABET[random_char_index as usize]);
    }

    return name;
}

#[test]
fn test_optional_columns() {
    // Setup the database.
    let db_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(db_file.path()).unwrap();
    conn.execute(CREATE_TABLE_STMT, params![]).unwrap();

    // Randomly pick Some or None variants for each row value.
    let mut rng = thread_rng();
    let mut expected_records = Vec::new();
    for _ in 0..16 {
        let a = match rng.next_u32() % 10 {
            x if x > 4 => Some(random_string()),
            _ => None,
        };

        let b = match rng.next_u32() {
            x if x > 4 => Some(random_integer()),
            _ => None,
        };

        let c = match rng.next_u32() {
            x if x > 4 => Some(random_real()),
            _ => None,
        };

        let d = match rng.next_u32() {
            x if x > 4 => Some(random_blob()),
            _ => None,
        };
        let row = TestRecordOpt { a, b, c, d };

        conn.execute(INSERT_STMT, params![row.a, row.b, row.c, row.d])
            .unwrap();
        expected_records.push(row);
    }
    conn.close().unwrap();

    // Deserialize the databse file and read the rows.
    let db_bytes = match std::fs::File::open(db_file.path()) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes
        }
        Err(err) => panic!("failed to open database file: {:?}", err),
    };

    let iter = from_bytes::<TestRecordOpt>(&db_bytes).unwrap();
    for (i, row) in iter.enumerate() {
        assert_eq!(expected_records[i], row.unwrap());
    }
}

#[test]
fn test_null_columns() {
    // Setup the database.
    let db_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(db_file.path()).unwrap();
    conn.execute(CREATE_TABLE_STMT, params![]).unwrap();

    // Randomly set some rows to null for each row value.
    let mut rng = thread_rng();
    let mut expected_records = Vec::new();
    for _ in 0..16 {
        match rng.next_u32() % 4 {
            0 => {
                let row = TestRecordOpt {
                    a: None,
                    b: Some(random_integer()),
                    c: Some(random_real()),
                    d: Some(random_blob()),
                };
                conn.execute(
                    INSERT_STMT,
                    params![rusqlite::types::Null, row.b, row.c, row.d],
                )
                .unwrap();
                expected_records.push(row);
            }
            1 => {
                let row = TestRecordOpt {
                    a: Some(random_string()),
                    b: None,
                    c: Some(random_real()),
                    d: Some(random_blob()),
                };
                conn.execute(
                    INSERT_STMT,
                    params![row.a, rusqlite::types::Null, row.c, row.d],
                )
                .unwrap();
                expected_records.push(row);
            }
            2 => {
                let row = TestRecordOpt {
                    a: Some(random_string()),
                    b: Some(random_integer()),
                    c: None,
                    d: Some(random_blob()),
                };
                conn.execute(
                    INSERT_STMT,
                    params![row.a, row.b, rusqlite::types::Null, row.d],
                )
                .unwrap();
                expected_records.push(row);
            }
            3 => {
                let row = TestRecordOpt {
                    a: Some(random_string()),
                    b: Some(random_integer()),
                    c: Some(random_real()),
                    d: None,
                };
                conn.execute(
                    INSERT_STMT,
                    params![row.a, row.b, row.c, rusqlite::types::Null],
                )
                .unwrap();
                expected_records.push(row);
            }
            _ => panic!(),
        };
    }
    conn.close().unwrap();

    // Deserialize the databse file and read the rows.
    let db_bytes = match std::fs::File::open(db_file.path()) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes
        }
        Err(err) => panic!("failed to open database file: {:?}", err),
    };

    let iter = from_bytes::<TestRecordOpt>(&db_bytes).unwrap();
    for (i, row) in iter.enumerate() {
        assert_eq!(expected_records[i], row.unwrap());
    }
}

#[test]
fn test_all_records_in_a_single_page() {
    // Setup the database.
    let db_file = NamedTempFile::new().unwrap();
    let mut conn = Connection::open(db_file.path()).unwrap();
    conn.execute(CREATE_TABLE_STMT, params![]).unwrap();

    // Insert a small number of random rows.
    let mut expected_records = Vec::new();
    for _ in 0..16 {
        let record = TestRecord {
            a: random_string(),
            b: random_integer(),
            c: random_real(),
            d: random_blob(),
        };

        conn.execute(INSERT_STMT, params![record.a, record.b, record.c, record.d])
            .unwrap();
        expected_records.push(record);
    }
    conn.close().unwrap();

    // Deserialize the database file and read the rows.
    let db_bytes = match std::fs::File::open(db_file.path()) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes
        }
        Err(err) => panic!("failed to open database file: {:?}", err),
    };

    let iter = from_bytes::<TestRecord>(&db_bytes).unwrap();
    for (i, row) in iter.enumerate() {
        assert_eq!(expected_records[i], row.unwrap());
    }
}

#[test]
fn test_records_spanning_multiple_pages() {
    // Setup the database.
    let db_file = NamedTempFile::new().unwrap();
    let mut conn = Connection::open(db_file.path()).unwrap();
    conn.execute(CREATE_TABLE_STMT, params![]).unwrap();

    // Insert a large number of random rows.
    let mut expected_records = Vec::new();
    for _ in 0..256 {
        let record = TestRecord {
            a: random_string(),
            b: random_integer(),
            c: random_real(),
            d: random_blob(),
        };

        conn.execute(INSERT_STMT, params![record.a, record.b, record.c, record.d])
            .unwrap();
        expected_records.push(record);
    }
    conn.close().unwrap();

    // Deserialize the database file and read the rows.
    let db_file_bytes = match std::fs::File::open(db_file.path()) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes
        }
        Err(err) => panic!("failed to open database file: {:?}", err),
    };

    let iter = from_bytes::<TestRecord>(&db_file_bytes).unwrap();
    for (i, row) in iter.enumerate() {
        assert_eq!(expected_records[i], row.unwrap());
    }
}
