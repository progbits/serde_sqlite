use byteorder::{BigEndian, ReadBytesExt};
use serde::Deserialize;
use serde::{
    de::{self, DeserializeSeed, IntoDeserializer, SeqAccess, Visitor},
    forward_to_deserialize_any,
};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::result::Result;

use crate::error::Error;

/// Decode a single variable length integer.
/// TODO: 7 and 8 byte variants.
fn decode_varint(input: &[u8]) -> (i64, usize) {
    // 1 byte varint.
    let mut a = input[0] as u64;
    if a & 0x80 == 0 {
        return (a as i64, 1);
    }

    // 2 byte varint.
    let mut b = input[1] as u64;
    if b & 0x80 == 0 {
        let a = (input[0] as u64 & 0x7f) << 7;
        let value = a | b;
        return (value as i64, 2);
    }

    // 3 byte varint.
    a = a << 14;
    a = a | input[2] as u64;
    if a & 0x80 == 0 {
        a &= (0x7f << 14) | (0x7f);
        b &= 0x7f;
        b = b << 7;
        return ((a | b) as i64, 3);
    }

    // 4 byte varint.
    let mut c = input[3] as u64;
    if c & 0x80 == 0 {
        a = a << 21;
        a = a | c;
        a &= (0x7f << 21) | (0x7f << 14) | (0x7f);
        b &= 0x7f;
        b = b << 7;
        c &= 0x7f;
        return ((a | b | c) as i64, 4);
    }

    // 5 byte varint.
    a = a << 28;
    a = a | input[4] as u64;
    if a & 0x80 == 0 {
        a &= (0x7f << 28) | (0x7f << 21) | (0x7f << 14) | (0x7f);
        b &= 0x7f;
        b = (b << 7) as u64;
        c &= 0x7f;
        c = (c << 7) as u64;
        return ((a | b | c) as i64, 5);
    }

    // 6 byte varint.
    a = a << 35;
    a = a | input[5] as u64;
    if a & 0x80 == 0 {
        a &= (0x7f << 35) | (0x7f << 28) | (0x7f << 21) | (0x7f << 14) | (0x7f);
        b &= 0x7f;
        b = (b << 7) as u64;
        c &= 0x7f;
        c = (c << 7) as u64;
        let d = (input[3] & 0x7f) as u64;
        return ((a | b | c | d) as i64, 6);
    }
    unimplemented!()
}

/// Represents the header at the start of the first page of a SQLite database file.
#[derive(Debug)]
struct DatabaseFileHeader {
    header_string: String,
    page_size: u16,
    file_format_write_version: u8,
    file_format_read_version: u8,
    unused_page_reserve: u8,
    maximum_embedded_payload_fraction: u8,
    minimum_embedded_payload_fraction: u8,
    leaf_payload_fraction: u8,
    file_change_counter: u32,
    database_size_pages: u32,
    first_freelist_trunk_page: u32,
    total_freelist_pages: u32,
    number_of_freelist_pages: u32,
    schema_cookie: u32,
    schema_format_number: u32,
    default_page_cache_size: u32,
    largest_vacuum_root_page: u32,
    database_text_encoding: u32,
    user_version: u32,
    zero_vacuum_mode: u32,
    application_id: u32,
    version_valid_for_number: u32,
    sqlite_version_number: u32,
}

impl DatabaseFileHeader {
    fn from_bytes(input: &[u8]) -> Self {
        DatabaseFileHeader {
            header_string: std::str::from_utf8(&input[0..16]).unwrap().to_owned(),
            page_size: (&input[16..]).read_u16::<BigEndian>().unwrap(),
            file_format_write_version: input[18],
            file_format_read_version: input[19],
            unused_page_reserve: input[20],
            maximum_embedded_payload_fraction: input[21],
            minimum_embedded_payload_fraction: input[22],
            leaf_payload_fraction: input[23],
            file_change_counter: (&input[24..]).read_u32::<BigEndian>().unwrap(),
            database_size_pages: (&input[28..]).read_u32::<BigEndian>().unwrap(),
            first_freelist_trunk_page: (&input[32..]).read_u32::<BigEndian>().unwrap(),
            total_freelist_pages: (&input[36..]).read_u32::<BigEndian>().unwrap(),
            number_of_freelist_pages: (&input[40..]).read_u32::<BigEndian>().unwrap(),
            schema_cookie: (&input[44..]).read_u32::<BigEndian>().unwrap(),
            schema_format_number: (&input[48..]).read_u32::<BigEndian>().unwrap(),
            default_page_cache_size: (&input[52..]).read_u32::<BigEndian>().unwrap(),
            largest_vacuum_root_page: (&input[56..]).read_u32::<BigEndian>().unwrap(),
            database_text_encoding: (&input[60..]).read_u32::<BigEndian>().unwrap(),
            user_version: (&input[64..]).read_u32::<BigEndian>().unwrap(),
            zero_vacuum_mode: (&input[68..]).read_u32::<BigEndian>().unwrap(),
            application_id: (&input[72..]).read_u32::<BigEndian>().unwrap(),
            version_valid_for_number: (&input[92..]).read_u32::<BigEndian>().unwrap(),
            sqlite_version_number: (&input[96..]).read_u32::<BigEndian>().unwrap(),
        }
    }
}

/// Represents a table B-Tree interior cell header.
#[derive(Debug)]
struct TableInteriorCellHeader {
    page_number_pointer: i64,
    row_id_key: i64,
}

impl TableInteriorCellHeader {
    /// Create a new `TableInteriorCellHeader` from a byte slice.
    fn from_bytes(input: &[u8]) -> (TableInteriorCellHeader, usize) {
        // Track how many bytes we have read.
        let mut read_offset = 0_usize;

        // Read the pointer (page number).
        let pointer = u32::from_be_bytes([
            input[read_offset],
            input[read_offset + 1],
            input[read_offset + 2],
            input[read_offset + 3],
        ]);
        read_offset += 4;

        // Read key (row id).
        let (key, bytes_read) = decode_varint(&input[read_offset..]);

        (
            TableInteriorCellHeader {
                page_number_pointer: pointer as i64,
                row_id_key: key,
            },
            bytes_read,
        )
    }
}

/// Represents a table B-Tree leaf cell header.
#[derive(Debug)]
struct TableLeafCellHeader<'a> {
    payload_bytes: i64,
    row_id: i64,
    // Payload, a portion of which may spill into subsequent pages.
    payload: &'a [u8],
    // first_overflow_page: u64,
}

impl<'a> TableLeafCellHeader<'a> {
    /// Create a new `TableLeafCellHeader` from a byte slice.
    fn from_bytes(input: &'a [u8]) -> TableLeafCellHeader<'a> {
        // Track how many bytes we have read.
        let mut read_offset = 0_usize;

        // Read the size of the cell contents.
        let (cell_content_size, bytes_read) = decode_varint(&input[read_offset..]);
        read_offset += bytes_read as usize;

        // This probably shouldn't be zero.
        if cell_content_size == 0 {
            panic!("zero sized cell");
        }

        // Read the row id key.
        let (row_id, bytes_read) = decode_varint(&input[read_offset..]);
        read_offset += bytes_read as usize;

        TableLeafCellHeader {
            payload_bytes: cell_content_size,
            row_id,
            payload: &input[read_offset..],
        }
    }
}

/// Represents the distinct page types.
#[derive(Debug, PartialEq)]
enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

/// Represents the header at the start of a page in SQLite database file.
#[derive(Debug)]
struct PageHeader {
    page_type: PageType,
    freeblock_offset: u16,
    number_of_cells: u16,
    cell_content_area_offset: u16,
    content_fragmented_free_bytes: u8,
    right_most_pointer_offset: Option<u32>, // Only present in interior pages.
}

impl PageHeader {
    /// Construct a new PageHeader from a byte slice.
    fn from_bytes(input: &[u8]) -> Self {
        let page_type = match input[0] {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            _ => panic!("invalid page type"),
        };

        // Only interior pages contain a `right_most_pointer_offset` field.
        let right_most_pointer_offset = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                let value = u32::from_be_bytes([input[8], input[9], input[10], input[11]]);
                Some(value)
            }
            _ => None,
        };

        PageHeader {
            page_type,
            freeblock_offset: u16::from_be_bytes([input[1], input[2]]),
            number_of_cells: u16::from_be_bytes([input[3], input[4]]),
            cell_content_area_offset: u16::from_be_bytes([input[5], input[6]]),
            content_fragmented_free_bytes: input[8],
            right_most_pointer_offset,
        }
    }
}

/// Represents a the value of a column in a record.
#[derive(Debug)]
enum ColumnValue {
    Null,
    Text(String),
    Integer(i64),
    Real(f64),
    Blob(Vec<u8>),
}

impl<'de> ColumnValue {
    fn any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            ColumnValue::Null => visitor.visit_none(),
            ColumnValue::Integer(value) => visitor.visit_i64(value),
            ColumnValue::Real(value) => visitor.visit_f64(value),
            ColumnValue::Text(value) => visitor.visit_string(value),
            ColumnValue::Blob(value) => visitor.visit_seq(value.into_deserializer()),
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for ColumnValue {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.any(visitor)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            ColumnValue::Integer(value) => visitor.visit_bool(value != 0),
            ColumnValue::Real(value) => visitor.visit_bool(value != 0.0),
            _ => self.any(visitor),
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            ColumnValue::Null => visitor.visit_f32(f32::NAN),
            _ => self.any(visitor),
        }
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            ColumnValue::Null => visitor.visit_f64(f64::NAN),
            _ => self.any(visitor),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        //visitor.visit_byte_buf(self)
        unimplemented!()
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            ColumnValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self {
            ColumnValue::Null => visitor.visit_unit(),
            _ => self.any(visitor),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        unimplemented!()
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        unimplemented!()
    }

    forward_to_deserialize_any! {
        i8 i16 i32 i64 u8 u16 u32 u64 char str string bytes
        newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}

/// Represents a SQLite record.
#[derive(Debug)]
pub struct SQLiteRecord<'de> {
    // Bytes of the represented record.
    input: &'de [u8],
    // Track how far we have read into the payload header.
    header_read_offset: usize,
    // Track how far we have read into the payload body.
    body_read_offset: usize,
}

impl<'de> SQLiteRecord<'de> {
    /// Create a new SQLiteRecordSeqAccess instance.
    fn new(input: &'de [u8]) -> SQLiteRecord<'de> {
        // Read the size of the header to determine the offset to the payload.
        let (record_header_size_bytes, bytes_read) = decode_varint(&input);

        SQLiteRecord {
            input,
            header_read_offset: bytes_read,
            body_read_offset: record_header_size_bytes as usize,
        }
    }

    /// Retrieve the next column value.
    fn next_value(&mut self) -> ColumnValue {
        // Decode the type of column from the record header and seek the header
        // offset to the first byte of the next column type, ready for the next
        // call.
        let (column_type, bytes_read) = decode_varint(&self.input[self.header_read_offset..]);
        self.header_read_offset += bytes_read;

        // Decode the column value and advance the body offset to the first byte
        // of the next value, ready for the next call.
        // Determine the size of the record.
        match column_type {
            0 => ColumnValue::Null,
            1 => {
                // Value is a 1B 2s compliment integer.
                let value = self.input[self.body_read_offset] as i8;
                self.body_read_offset += 1;
                ColumnValue::Integer(value as i64)
            }
            2 => {
                // Value is a big-endian 2B 2s compliment integer.
                let value = i16::from_be_bytes(
                    self.input[self.body_read_offset..self.body_read_offset + 2]
                        .try_into()
                        .expect("incorrect sized slice"),
                );
                self.body_read_offset += 2;
                ColumnValue::Integer(value as i64)
            }
            3 => {
                // Value is a big-endian 3B 2s compliment integer.
                let mask: u8 = if self.input[self.body_read_offset + 0] & 0x80 > 0 {
                    0xff
                } else {
                    0x00
                };
                let bytes = [
                    mask,
                    mask,
                    mask,
                    mask,
                    mask,
                    self.input[self.body_read_offset + 0],
                    self.input[self.body_read_offset + 1],
                    self.input[self.body_read_offset + 2],
                ];

                let value = i64::from_be_bytes(bytes);
                self.body_read_offset += 3;
                ColumnValue::Integer(value)
            }
            4 => {
                // Value is a big-endian 4B 2s compliment integer.
                let value = i32::from_be_bytes(
                    self.input[self.body_read_offset..self.body_read_offset + 4]
                        .try_into()
                        .expect("incorrect sized slice"),
                );
                self.body_read_offset += 4;
                ColumnValue::Integer(value as i64)
            }
            5 => {
                // Value is a big-endian 6B 2s compliment integer.
                let mask: u8 = if self.input[self.body_read_offset + 0] & 0x80 > 0 {
                    0xff
                } else {
                    0x00
                };
                let bytes = [
                    mask,
                    mask,
                    self.input[self.body_read_offset + 0],
                    self.input[self.body_read_offset + 1],
                    self.input[self.body_read_offset + 2],
                    self.input[self.body_read_offset + 3],
                    self.input[self.body_read_offset + 4],
                    self.input[self.body_read_offset + 5],
                ];

                let value = i64::from_be_bytes(bytes);
                self.body_read_offset += 6;
                ColumnValue::Integer(value as i64)
            }
            6 => {
                // Value is a big-endian 8B 2s compliment integer.
                let value = i64::from_be_bytes(
                    self.input[self.body_read_offset..self.body_read_offset + 8]
                        .try_into()
                        .expect("incorrect sized slice"),
                );
                self.body_read_offset += 8;
                ColumnValue::Integer(value)
            }
            7 => {
                // Value is a big-endian IEEE 754-2008 8B floating point number.
                let value = f64::from_be_bytes(
                    self.input[self.body_read_offset..self.body_read_offset + 8]
                        .try_into()
                        .expect("incorrect sized slice"),
                );
                self.body_read_offset += 8;
                ColumnValue::Real(value)
            }
            8 => {
                // The value is 0 and 0B in size.
                ColumnValue::Integer(0)
            }
            9 => {
                // The value is 1 and 0B in size.
                ColumnValue::Integer(1)
            }
            10..=11 => {
                // This should never happen.
                panic!("invalid record type")
            }
            _ => {
                if column_type % 2 == 0 {
                    // Value is a BLOB that is (column_type-12)/2 bytes in length.
                    let size = ((column_type - 12) / 2) as usize;
                    let value = self.input[self.body_read_offset..]
                        .iter()
                        .take(size)
                        .map(|value| *value)
                        .collect();
                    self.body_read_offset += size;
                    ColumnValue::Blob(value)
                } else {
                    // Value is a string that is (column_type-13)/2 bytes in length.
                    let size = ((column_type - 13) / 2) as usize;
                    let value = std::str::from_utf8(
                        &self.input[self.body_read_offset..self.body_read_offset + size],
                    )
                    .unwrap()
                    .parse()
                    .unwrap();
                    self.body_read_offset += size;
                    ColumnValue::Text(value)
                }
            }
        }
    }
}

impl<'de> SeqAccess<'de> for SQLiteRecord<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        let column = self.next_value();
        seed.deserialize(column).map(Some)
    }
}

/// Represents a type that can deserialize records from SQLite database files.
#[derive(Debug)]
pub struct SQLiteDeserializer<'de, T> {
    // Bytes of the SQLite file being deserialized.
    input: &'de [u8],
    // SQLite database file header.
    database_file_header: DatabaseFileHeader,
    // Map tables to the leaf pages which contain their data.
    table_leaf_page_map: HashMap<String, Vec<i64>>,
    // Leaf pages containing records for the table being deserialized.
    leaf_pages: VecDeque<u64>,
    // Current page being decoded.
    current_page_index: u64,
    // Byte offset to the next cell pointer to be decoded.
    page_cell_pointer_offset: usize,
    // Current offset into `input`.
    offset: usize,
    // Number of records remaining in the current page.
    leaf_records_remaining: u16,
    // Index of the record currently being deserialized.
    record_idx: u16,
    // Do we already know the table we are decoding?
    is_setup: bool,
    // Have we read all records.
    is_done: bool,
    data: PhantomData<T>,
}

impl<'de, T> Iterator for SQLiteDeserializer<'de, T>
where
    T: Deserialize<'de>,
{
    type Item = Result<T, Error>;

    /// Advances the iterator and returns the next value.
    ///
    /// Returns [`None`] when iteration is finished. Once iteration has finished, the iterator
    /// cannot be resumed.
    fn next(&mut self) -> Option<Self::Item> {
        // Because `SQLiteDeserializer::from_bytes(...)` lazily constructs a new
        // instance, the first time `SQLiteDeserializer::next(...)` is called
        // we do not know where we need to start reading data from.
        if !self.is_setup {
            // Call `deserialize` to run setup and deserialize a record (if the
            // table isn't empty).
            return Some(T::deserialize(self));
        }

        // If we are here, we have already been setup. If we have records
        // remaining in the current page, deserialize the next record.
        if self.leaf_records_remaining > 0 {
            return Some(T::deserialize(self));
        } else {
            // If we have further leaf pages remaining to decode records from,
            // setup to read the next page and deserialize the next record.
            if !self.leaf_pages.is_empty() {
                self.next_page();
                return Some(T::deserialize(self));
            }

            // We have run out of records and pages, we are done. This iterator
            // cannot be resumed, so from now on, we will only return None.
            return None;
        }
    }
}

impl<'de, T> SQLiteDeserializer<'de, T> {
    /// Create a new SQLiteDeserializer from a byte slice.
    pub fn from_bytes(input: &'de [u8]) -> Self {
        // Read the database file header.
        let header = DatabaseFileHeader::from_bytes(&input);

        // Extract the leaf pages for each table in the database. Maybe move this into `do_setup()`
        // so that construction is completely lazy.
        let table_leaf_page_map = Self::build_leaf_table_page_map(&input);

        SQLiteDeserializer {
            input,
            database_file_header: header,
            page_cell_pointer_offset: 0,
            table_leaf_page_map,
            leaf_pages: VecDeque::new(),
            current_page_index: 0,
            offset: 0,
            leaf_records_remaining: 0,
            record_idx: 0,
            is_setup: false,
            is_done: false,
            data: PhantomData,
        }
    }

    /// Setup to read records from a table.
    ///
    /// This method is typically called during the first call to `deserialize_struct(...)`. It
    /// sets up internal state for reading records from the leaf pages associated with `table`.
    fn do_setup(&mut self, table: &str) {
        // Setup the leaf pages containing for the table represented by `name`.
        // TODO: Use From
        let mut deque = VecDeque::new();
        for value in self.table_leaf_page_map[table].iter() {
            deque.push_back(*value as u64)
        }
        self.leaf_pages = deque;

        // Seek to the next page to deserialize records from.
        self.next_page();

        self.is_setup = true;
    }

    /// Setup to read the next page of records.
    ///
    /// Seeks to the first record of the page and sets the number of records this page contains
    /// to decode.
    fn next_page(&mut self) {
        // Set the index of the page currently being deserialized.
        self.current_page_index = match self.leaf_pages.pop_front() {
            Some(value) => value,
            None => panic!("no leaf pages left to decode"),
        };

        // Read the page header to determine how many cells we need to decode.
        let page_offset =
            self.database_file_header.page_size as u64 * (self.current_page_index - 1);
        let page_header = PageHeader::from_bytes(&self.input[page_offset as usize..]);
        self.leaf_records_remaining = page_header.number_of_cells;

        // Set the offset to the first cell pointer, this is the cell we will start deserializing
        // records from.
        self.page_cell_pointer_offset = match page_header.page_type {
            PageType::LeafTable => (page_offset + 8) as usize,
            _ => panic!("expected leaf page"),
        };
    }

    /// Build a map of leaf pages for each table in the database.
    fn build_leaf_table_page_map(input: &'de [u8]) -> HashMap<String, Vec<i64>> {
        // Decode the file header.
        let file_header = DatabaseFileHeader::from_bytes(&input);

        // Decode the page header. The first page header follows a 100B file header.
        let page_header_offset = 100_usize;
        let page_header = PageHeader::from_bytes(&input[page_header_offset..]);

        // Following the page header is the offset (in bytes) to the first cell pointer.
        let cell_pointer_offset = match page_header.page_type {
            PageType::LeafTable => 108, // 100B file header + 8B page header.
            _ => panic!("first page should always be PageType::LeafTable"),
        };

        // For each table present in the master table, add an entry to `map`. The payload of
        // each cell of the master table should be a record collection in the form of the
        // The payload of each cell in the first page should be a row of the master table.
        //
        // CREATE TABLE sqlite_master(
        //     type text,
        //     name text,
        //     tbl_name text,
        //     rootpage integer,
        //     sql text
        // );
        //
        // With one entry for each table in the database. We want to build a mapping between
        // the name of each table and the rootpage of that table.
        let mut table_root_page_map = HashMap::new();
        for i in 0..page_header.number_of_cells {
            // Read the value of the cell pointer to get the cell data offset.
            let cell_pointer_offset = (cell_pointer_offset + i * 2) as usize;
            let cell_data_offset = (&input[cell_pointer_offset..])
                .read_u16::<BigEndian>()
                .unwrap();

            // Read the cell header.
            let cell_header = match page_header.page_type {
                PageType::LeafTable => {
                    TableLeafCellHeader::from_bytes(&input[cell_data_offset as usize..])
                }
                _ => panic!("unsupported page type"), // Currently only deal with leaf cells.
            };

            let (payload, _) = SQLiteDeserializer::<T>::decode_payload(&cell_header.payload);

            // Extract the table name and root page for the table from the schema.
            if payload.len() != 5 {
                std::process::exit(1);
            }

            let table_name = match &payload[1] {
                ColumnValue::Text(value) => value.clone(),
                _ => {
                    std::process::exit(1);
                }
            };

            let root_page = match &payload[3] {
                ColumnValue::Integer(value) => *value,
                _ => {
                    std::process::exit(1);
                }
            };

            table_root_page_map.insert(table_name, root_page);
        }

        // Now that we have the root page for each table in the database, we want to build a
        // mapping between each table in the database and the corresponding data containing leaf
        // tables. For small tables, the root table might be the leaf table. For larger tables, we
        // will have to traverse the B-Tree and extract the root page indices.
        let mut table_leaf_page_map = HashMap::new();
        for (table, root_page) in table_root_page_map.iter() {
            // Read the header of the root page for the current table.
            let root_page_offset = ((root_page - 1) * file_header.page_size as i64) as usize;
            let page_header = PageHeader::from_bytes(&input[root_page_offset..]);
            match page_header.page_type {
                PageType::InteriorTable => {
                    // Traverse the B-Tree and extract data containing leaf pages.
                    let mut leaf_pages = Vec::new();
                    SQLiteDeserializer::<T>::extract_leaf_pages(
                        file_header.page_size as usize,
                        *root_page,
                        &mut leaf_pages,
                        &input,
                    );

                    table_leaf_page_map.insert(table.clone(), leaf_pages);
                }
                PageType::LeafTable => {
                    // Root page is a leaf page
                    table_leaf_page_map.insert(table.clone(), vec![*root_page]);
                }
                _ => panic!("non table page type: {:?}", page_header.page_type),
            }
        }

        table_leaf_page_map
    }

    /// Recursivly traverse a B-Tree and extract leaf pages.
    fn extract_leaf_pages(
        page_size: usize,
        page_number: i64,
        leaf_pages: &mut Vec<i64>,
        input: &[u8],
    ) {
        let page_offset = (page_number - 1) as usize * page_size;
        let page_header = PageHeader::from_bytes(&input[page_offset..]);
        match page_header.page_type {
            PageType::InteriorTable => {
                for cell_index in 0..page_header.number_of_cells {
                    let cell_pointer_offset = page_offset + 12 + cell_index as usize * 2;
                    let cell_data_offset = (&input[cell_pointer_offset..])
                        .read_u16::<BigEndian>()
                        .unwrap() as usize
                        + page_offset;

                    // Read the cell header and calculate the offset to the pointed page.
                    let (cell_header, _) =
                        TableInteriorCellHeader::from_bytes(&input[cell_data_offset as usize..]);

                    SQLiteDeserializer::<T>::extract_leaf_pages(
                        page_size,
                        cell_header.page_number_pointer,
                        leaf_pages,
                        &input,
                    );
                }

                // After we have handled all of the cells, handle the right most pointer.
                SQLiteDeserializer::<T>::extract_leaf_pages(
                    page_size,
                    page_header.right_most_pointer_offset.unwrap() as i64,
                    leaf_pages,
                    &input,
                );
            }
            PageType::LeafTable => {
                leaf_pages.push(page_number);
                return;
            }
            _ => panic!("only expected table pages"),
        }
    }

    /// Decode the payload of a table leaf cell.
    fn decode_payload(input: &[u8]) -> (Vec<ColumnValue>, usize) {
        // Track the offset into `input` as we decode the payload.
        let mut read_offset = 0;

        // Decode the payload header.
        let (record_header_bytes, bytes_read) = decode_varint(&input[read_offset..]);
        read_offset += bytes_read as usize;

        // Decode the payload for the cell.
        let mut column_types = Vec::new();
        let mut record_header_bytes_remaining = record_header_bytes - bytes_read as i64;
        loop {
            if record_header_bytes_remaining <= 0 {
                break;
            }
            let (type_id, bytes_read) = decode_varint(&input[read_offset..]);
            column_types.push(type_id);
            read_offset += bytes_read;

            record_header_bytes_remaining -= bytes_read as i64;
        }

        // Decode each of the records.
        let mut records = Vec::new();
        for value in column_types {
            match value {
                0 => {} // Null value
                1 => {
                    let value = input[read_offset];
                    records.push(ColumnValue::Integer(value as i64));
                    read_offset += 1;
                }
                2 => {
                    let value = i64::from_be_bytes([
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        input[read_offset],
                        input[read_offset + 1],
                    ]);
                    records.push(ColumnValue::Integer(value));
                    read_offset += 2;
                }
                3 => {
                    let value = i64::from_be_bytes([
                        0,
                        0,
                        0,
                        0,
                        0,
                        input[read_offset],
                        input[read_offset + 1],
                        input[read_offset + 2],
                    ]);
                    records.push(ColumnValue::Integer(value));
                    read_offset += 3;
                }
                4 => {
                    let value = i64::from_be_bytes([
                        0,
                        0,
                        0,
                        0,
                        input[read_offset],
                        input[read_offset + 1],
                        input[read_offset + 2],
                        input[read_offset + 3],
                    ]);
                    records.push(ColumnValue::Integer(value));
                    read_offset += 4;
                }
                5 => {
                    let value = i64::from_be_bytes([
                        0,
                        0,
                        input[read_offset],
                        input[read_offset + 1],
                        input[read_offset + 2],
                        input[read_offset + 3],
                        input[read_offset + 4],
                        input[read_offset + 5],
                    ]);
                    records.push(ColumnValue::Integer(value));
                    read_offset += 6;
                }
                6 => {
                    let value = i64::from_be_bytes([
                        input[read_offset],
                        input[read_offset + 1],
                        input[read_offset + 2],
                        input[read_offset + 3],
                        input[read_offset + 4],
                        input[read_offset + 5],
                        input[read_offset + 6],
                        input[read_offset + 7],
                    ]);
                    records.push(ColumnValue::Integer(value));
                    read_offset += 8;
                }
                7 => {
                    let value = f64::from_be_bytes([
                        input[read_offset],
                        input[read_offset + 1],
                        input[read_offset + 2],
                        input[read_offset + 3],
                        input[read_offset + 4],
                        input[read_offset + 5],
                        input[read_offset + 6],
                        input[read_offset + 7],
                    ]);
                    records.push(ColumnValue::Real(value));
                    read_offset += 8;
                }
                8 => {
                    records.push(ColumnValue::Integer(0));
                }
                9 => {
                    records.push(ColumnValue::Integer(1));
                }
                10..=11 => {
                    panic!("invalid record type");
                }
                _ => {
                    if value % 2 == 0 {
                        let blob_size = ((value - 12) / 2) as usize;
                        let value = Vec::from(&input[read_offset..read_offset + blob_size]);
                        records.push(ColumnValue::Blob(value));
                        read_offset += blob_size;
                    } else {
                        let string_size = ((value - 13) / 2) as usize;
                        let value =
                            std::str::from_utf8(&input[read_offset..read_offset + string_size])
                                .unwrap()
                                .to_owned();
                        records.push(ColumnValue::Text(value));
                        read_offset += string_size;
                    }
                }
            }
        }

        // Return the number of bytes read to de-code the record.
        (records, read_offset)
    }
}

/// Return an iterator that yields instances of T.
pub fn from_bytes<'a, T>(input: &'a [u8]) -> Result<SQLiteDeserializer<T>, Error>
where
    T: Deserialize<'a>,
{
    // Create a new SQLiteDeserializer instance. This instance now has
    // knowledge of the master table and thus the mapping of tables to pages.
    let deserializer = SQLiteDeserializer::<T>::from_bytes(input);
    Ok(deserializer)
}

impl<'de, 'a, T> de::Deserializer<'de> for &'a mut SQLiteDeserializer<'de, T> {
    /// The error type returned if an error is encountered during deserialization.
    type Error = Error;

    /// Require the `Deserializer` to figure out how to drive the visitor based
    /// on what data type is in the input.
    ///
    /// When implementing `Deserialize`, you should avoid relying on
    /// `Deserializer::deserialize_any` unless you need to be told by the
    /// Deserializer what type is in the input. Know that relying on
    /// `Deserializer::deserialize_any` means your data type will be able to
    /// deserialize from self-describing formats only, ruling out Bincode and
    /// many others.
    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `bool` value.
    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting an `i8` value.
    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting an `i16` value.
    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting an `i32` value.
    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting an `i64` value.
    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `u8` value.
    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `u16` value.
    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `u32` value.
    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `u64` value.
    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `f32` value.
    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `f64` value.
    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a `char` value.
    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a string value and does
    /// not benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor` would benefit from taking ownership of `String` data,
    /// indiciate this to the `Deserializer` by using `deserialize_string`
    /// instead.
    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a string value and would
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor` would not benefit from taking ownership of `String`
    /// data, indicate that to the `Deserializer` by using `deserialize_str`
    /// instead.
    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a byte array and does not
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor` would benefit from taking ownership of `Vec<u8>` data,
    /// indicate this to the `Deserializer` by using `deserialize_byte_buf`
    /// instead.
    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a byte array and would
    /// benefit from taking ownership of buffered data owned by the
    /// `Deserializer`.
    ///
    /// If the `Visitor` would not benefit from taking ownership of `Vec<u8>`
    /// data, indicate that to the `Deserializer` by using `deserialize_bytes`
    /// instead.
    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting an optional value.
    ///
    /// This allows deserializers that encode an optional value as a nullable
    /// value to convert the null value into `None` and a regular value into
    /// `Some(value)`.
    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a unit value.
    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a unit struct with a
    /// particular name.
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a newtype struct with a
    /// particular name.
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a sequence of values.
    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a sequence of values and
    /// knows how many values there are without looking at the serialized data.
    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a tuple struct with a
    /// particular name and number of fields.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a map of key-value pairs.
    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting a struct with a particular
    /// name and fields.
    fn deserialize_struct<V>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // This method is called every time we call `next()`. The first time `next()` is called is
        // when we find out the name of the table being deserialized. It is at this point we setup
        // Self to decode records from this table. This setup doesn't need to happen on subsequent
        // calls to next.
        if !self.is_setup {
            // After setup, we are immediately ready to start decoding records.
            self.do_setup(name);
        }

        // Read the next cell data pointer to get the cell data offset.
        let offset_to_cell_data = u16::from_be_bytes([
            self.input[self.page_cell_pointer_offset],
            self.input[self.page_cell_pointer_offset + 1],
        ]);

        // Decrement the count fo records remaining in this page and advance
        // the cell pointer offset 2B to the next cell pointer, ready to
        // deserialize the next record.
        self.leaf_records_remaining -= 1;
        self.page_cell_pointer_offset += 2;

        // Read the number of payload bytes and the row id from the leaf cell header.
        let page_offset =
            (self.current_page_index - 1) * self.database_file_header.page_size as u64;
        let mut read_offset = (page_offset + offset_to_cell_data as u64) as usize;
        let (_, bytes_read) = decode_varint(&self.input[read_offset..]);
        read_offset += bytes_read;

        // Decode the row id.
        let (_, bytes_read) = decode_varint(&self.input[read_offset..]);
        read_offset += bytes_read;

        // Return a new sequential visitor to decode the payload contents.
        let record_seq_access = SQLiteRecord::new(&self.input[read_offset..]);
        visitor.visit_seq(record_seq_access)
    }

    /// Hint that the `Deserialize` type is expecting an enum value with a
    /// particular name and possible variants.
    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type is expecting the name of a struct
    /// field or the discriminant of an enum variant.
    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    /// Hint that the `Deserialize` type needs to deserialize a value whose type
    /// doesn't matter because it is ignored.
    ///
    /// Deserializers for non-self-describing formats may not support this mode.
    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}
