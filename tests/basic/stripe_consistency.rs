// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::proto::column_encoding::Kind as EncodingKind;
use orc_rust::proto::r#type::Kind as TypeKind;
use orc_rust::proto::stream::Kind;
use orc_rust::proto::{
    BloomFilter, BloomFilterIndex, ColumnEncoding, ColumnStatistics, CompressionKind, Footer,
    IntegerStatistics, PostScript, RowIndex, RowIndexEntry, Stream, StripeFooter,
    StripeInformation, Type,
};
use orc_rust::stripe::Stripe;
use prost::Message;

const MAGIC_LENGTH: u64 = 3;

fn node(kind: TypeKind, subtypes: &[u32], field_names: &[&str]) -> Type {
    Type {
        kind: Some(kind as i32),
        subtypes: subtypes.to_vec(),
        field_names: field_names.iter().map(|name| name.to_string()).collect(),
        ..Default::default()
    }
}

fn int_and_string() -> Vec<Type> {
    vec![
        node(TypeKind::Struct, &[1, 2], &["a", "s"]),
        node(TypeKind::Int, &[], &[]),
        node(TypeKind::String, &[], &[]),
    ]
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = vec![];
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

fn literal_run(values: &[u64]) -> Vec<u8> {
    let mut out = vec![(values.len() as i8).wrapping_neg() as u8];
    for &value in values {
        out.extend(varint(value));
    }
    out
}

fn signed_run(values: &[i64]) -> Vec<u8> {
    literal_run(&values.iter().map(|&v| zigzag(v)).collect::<Vec<_>>())
}

struct StripeSpec {
    rows: u64,
    index: u64,
    data: u64,
    body: Vec<u8>,
    footer: StripeFooter,
}

fn encoding(kind: EncodingKind, dictionary_size: Option<u32>) -> ColumnEncoding {
    ColumnEncoding {
        kind: Some(kind as i32),
        dictionary_size,
        ..Default::default()
    }
}

fn stripe(rows: u64, streams: &[(u32, Kind, Vec<u8>)], encodings: &[ColumnEncoding]) -> StripeSpec {
    let is_index = |kind: Kind| {
        matches!(
            kind,
            Kind::RowIndex | Kind::BloomFilter | Kind::BloomFilterUtf8
        )
    };
    let mut spec = StripeSpec {
        rows,
        index: 0,
        data: 0,
        body: vec![],
        footer: StripeFooter {
            columns: encodings.to_vec(),
            ..Default::default()
        },
    };
    for (column, kind, bytes) in streams {
        if is_index(*kind) {
            spec.index += bytes.len() as u64;
        } else {
            spec.data += bytes.len() as u64;
        }
        spec.body.extend_from_slice(bytes);
        spec.footer.streams.push(Stream {
            kind: Some(*kind as i32),
            column: Some(*column),
            length: Some(bytes.len() as u64),
        });
    }
    spec
}

fn direct(columns: usize) -> Vec<ColumnEncoding> {
    vec![encoding(EncodingKind::Direct, None); columns]
}

fn numbered_rows(first: i64, rows: usize) -> StripeSpec {
    let numbers = (first..first + rows as i64).collect::<Vec<_>>();
    let letters = (0..rows)
        .map(|row| b'a' + (first as usize + row) as u8 % 26)
        .collect::<Vec<_>>();
    stripe(
        rows as u64,
        &[
            (1, Kind::Data, signed_run(&numbers)),
            (2, Kind::Length, literal_run(&vec![1; rows])),
            (2, Kind::Data, letters),
        ],
        &direct(3),
    )
}

struct OrcFile {
    types: Vec<Type>,
    stripes: Vec<StripeSpec>,
    row_index_stride: Option<u32>,
    compression: CompressionKind,
    block_size: Option<u64>,
}

impl OrcFile {
    fn new(stripes: Vec<StripeSpec>) -> Self {
        Self {
            types: int_and_string(),
            stripes,
            row_index_stride: None,
            compression: CompressionKind::None,
            block_size: None,
        }
    }

    fn section(&self, raw: Vec<u8>) -> Vec<u8> {
        if self.compression == CompressionKind::None || raw.is_empty() {
            return raw;
        }
        let mut out = (((raw.len() as u32) << 1) | 1).to_le_bytes()[..3].to_vec();
        out.extend(raw);
        out
    }

    fn render(&self) -> Bytes {
        self.render_with(|_, _| {})
    }

    fn render_with(&self, edit: impl FnOnce(&mut Footer, &mut PostScript)) -> Bytes {
        let mut file = b"ORC".to_vec();
        let mut footer = Footer {
            header_length: Some(MAGIC_LENGTH),
            types: self.types.clone(),
            row_index_stride: self.row_index_stride,
            ..Default::default()
        };
        for spec in &self.stripes {
            let offset = file.len() as u64;
            file.extend_from_slice(&spec.body);
            let stripe_footer = self.section(spec.footer.encode_to_vec());
            file.extend_from_slice(&stripe_footer);
            footer.stripes.push(StripeInformation {
                offset: Some(offset),
                index_length: Some(spec.index),
                data_length: Some(spec.data),
                footer_length: Some(stripe_footer.len() as u64),
                number_of_rows: Some(spec.rows),
                ..Default::default()
            });
        }
        footer.content_length = Some(file.len() as u64 - MAGIC_LENGTH);
        footer.number_of_rows = Some(self.stripes.iter().map(|spec| spec.rows).sum());
        let mut postscript = PostScript {
            compression: Some(self.compression as i32),
            compression_block_size: self.block_size,
            version: vec![0, 12],
            metadata_length: Some(0),
            magic: Some("ORC".to_string()),
            ..Default::default()
        };
        edit(&mut footer, &mut postscript);
        let footer = self.section(footer.encode_to_vec());
        postscript.footer_length = postscript.footer_length.or(Some(footer.len() as u64));
        file.extend_from_slice(&footer);
        let postscript = postscript.encode_to_vec();
        file.extend_from_slice(&postscript);
        file.push(postscript.len() as u8);
        Bytes::from(file)
    }
}

fn read(file: Bytes, batch_size: usize) -> Result<Vec<RecordBatch>, String> {
    ArrowReaderBuilder::try_new(file)
        .map_err(|error| error.to_string())?
        .with_batch_size(batch_size)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
}

fn numbers_and_letters(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut out = vec![];
    for batch in batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(a.is_valid(row) && s.is_valid(row));
            out.push((a.value(row), s.value(row).to_string()));
        }
    }
    out
}

fn expected(first: i32, rows: i32) -> Vec<(i32, String)> {
    (first..first + rows)
        .map(|n| (n, ((b'a' + n as u8 % 26) as char).to_string()))
        .collect()
}

fn assert_reads(file: Bytes, expected: Vec<(i32, String)>) {
    for batch_size in [1, 8192] {
        let batches = read(file.clone(), batch_size).unwrap();
        assert_eq!(numbers_and_letters(&batches), expected);
    }
}

fn assert_refused(file: Bytes, message: &str) {
    for batch_size in [1, 8192] {
        let error = read(file.clone(), batch_size).expect_err("damaged file must be refused");
        assert!(error.contains(message), "{error}");
    }
}

fn two_stripes() -> OrcFile {
    OrcFile::new(vec![numbered_rows(0, 5), numbered_rows(5, 4)])
}

#[test]
fn test_hand_assembled_file_reads() {
    assert_reads(
        OrcFile::new(vec![numbered_rows(0, 5)]).render(),
        expected(0, 5),
    );
}

#[test]
fn test_hand_assembled_multi_stripe_file_reads() {
    assert_reads(two_stripes().render(), expected(0, 9));
}

#[test]
fn test_zero_row_stripe_between_stripes_reads() {
    let empty = stripe(0, &[], &direct(3));
    let file = OrcFile::new(vec![numbered_rows(0, 5), empty, numbered_rows(5, 4)]);
    assert_reads(file.render(), expected(0, 9));
}

#[test]
fn test_file_without_stripes_reads() {
    let file = OrcFile::new(vec![]).render();
    assert!(read(file, 8192).unwrap().is_empty());
}

#[test]
#[ignore = "stream lengths are not compared with the stripe lengths yet"]
fn test_stream_longer_than_declared_is_refused() {
    let mut file = two_stripes();
    let length = file.stripes[0].footer.streams[0].length();
    file.stripes[0].footer.streams[0].length = Some(length + 1);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
#[ignore = "stream lengths are not compared with the stripe lengths yet"]
fn test_stream_shorter_than_declared_is_refused() {
    let mut file = two_stripes();
    let length = file.stripes[0].footer.streams[2].length();
    file.stripes[0].footer.streams[2].length = Some(length - 1);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
#[ignore = "stream lengths are not compared with the stripe lengths yet"]
fn test_index_and_data_lengths_that_swap_bytes_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        let stripe = &mut footer.stripes[0];
        stripe.index_length = Some(stripe.index_length() + 1);
        stripe.data_length = Some(stripe.data_length() - 1);
    });
    assert_refused(file, "stripe footer streams hold");
}

#[test]
#[ignore = "stream lengths are not compared with the stripe lengths yet"]
fn test_stream_length_zeroed_in_the_stripe_footer_is_refused() {
    let mut file = two_stripes();
    file.stripes[1].footer.streams[0].length = Some(0);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
#[ignore = "duplicate streams are not refused yet"]
fn test_duplicate_data_stream_on_an_int_column_is_refused() {
    let replaced = stripe(
        5,
        &[
            (1, Kind::Data, signed_run(&[0, 1, 2, 3, 4])),
            (1, Kind::Data, signed_run(&[9, 9, 9, 9, 9])),
            (2, Kind::Length, literal_run(&[1; 5])),
            (2, Kind::Data, b"abcde".to_vec()),
        ],
        &direct(3),
    );
    assert_refused(
        OrcFile::new(vec![replaced]).render(),
        "column 1 has more than one Data stream",
    );
}

#[test]
#[ignore = "duplicate streams are not refused yet"]
fn test_duplicate_length_stream_on_a_string_column_is_refused() {
    let doubled = stripe(
        5,
        &[
            (1, Kind::Data, signed_run(&[0, 1, 2, 3, 4])),
            (2, Kind::Length, literal_run(&[1; 5])),
            (2, Kind::Length, literal_run(&[2; 5])),
            (2, Kind::Data, b"abcde".to_vec()),
        ],
        &direct(3),
    );
    assert_refused(
        OrcFile::new(vec![doubled]).render(),
        "column 2 has more than one Length stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_length_stream_on_an_int_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(Kind::Length as i32);
    assert_refused(
        file.render(),
        "column 1 of type Some(Int) cannot have a Length stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_secondary_stream_on_a_string_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[1].kind = Some(Kind::Secondary as i32);
    assert_refused(
        file.render(),
        "column 2 of type Some(String) cannot have a Secondary stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_data_stream_on_a_struct_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].column = Some(0);
    assert_refused(
        file.render(),
        "column 0 of type Some(Struct) cannot have a Data stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_dictionary_data_stream_on_an_int_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(Kind::DictionaryData as i32);
    assert_refused(
        file.render(),
        "column 1 of type Some(Int) cannot have a DictionaryData stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_stream_of_a_column_beyond_the_type_tree_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].column = Some(9);
    assert_refused(
        file.render(),
        "column 9 of type None cannot have a Data stream",
    );
}

#[test]
#[ignore = "stream kinds are not checked against the column type yet"]
fn test_stream_of_unknown_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(77);
    assert_refused(file.render(), "column 1 has a stream of unknown kind 77");
}

#[test]
#[ignore = "encoding kinds are not checked yet"]
fn test_encoding_of_unknown_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.columns[1].kind = Some(9);
    assert_refused(file.render(), "column 1 has unknown encoding kind 9");
}

#[test]
#[ignore = "the footer row count is not compared with its stripes yet"]
fn test_footer_row_count_above_the_stripes_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.number_of_rows = Some(footer.number_of_rows() + 1);
    });
    assert_refused(file, "Footer states 10 rows but its stripes hold 9");
}

#[test]
#[ignore = "the footer row count is not compared with its stripes yet"]
fn test_footer_row_count_below_the_stripes_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.number_of_rows = Some(footer.number_of_rows() - 1);
    });
    assert_refused(file, "Footer states 8 rows but its stripes hold 9");
}

#[test]
#[ignore = "the footer row count is not compared with its stripes yet"]
fn test_stripe_row_count_contradicting_the_footer_total_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].number_of_rows = Some(5);
    });
    assert_refused(file, "Footer states 9 rows but its stripes hold 10");
}

#[test]
#[ignore = "the footer row count is not compared with its stripes yet"]
fn test_footer_without_stripes_but_with_rows_is_refused() {
    let file = OrcFile::new(vec![]).render_with(|footer, _| {
        footer.number_of_rows = Some(3);
    });
    let error = read(file, 8192).expect_err("rows without stripes must be refused");
    assert!(
        error.contains("Footer states 3 rows but its stripes hold 0"),
        "{error}"
    );
}

#[test]
#[ignore = "stripe placement is not validated yet"]
fn test_stripe_ending_beyond_the_file_tail_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].data_length = Some(footer.stripes[1].data_length() + 1000);
    });
    assert_refused(file, "Stripe 1 ends at");
}

#[test]
#[ignore = "stripe placement is not validated yet"]
fn test_overlapping_stripes_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].offset = Some(footer.stripes[0].offset() + 1);
    });
    assert_refused(file, "Stripe 1 starts at");
}

#[test]
#[ignore = "stripe placement is not validated yet"]
fn test_stripes_listed_out_of_order_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes.swap(0, 1);
    });
    assert_refused(file, "Stripe 1 starts at");
}

#[test]
#[ignore = "stripe placement is not validated yet"]
fn test_stripe_starting_inside_the_magic_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[0].offset = Some(0);
    });
    assert_refused(file, "Stripe 0 starts at 0");
}

#[test]
#[ignore = "stripe placement is not validated yet"]
fn test_stripe_extent_overflow_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].offset = Some(u64::MAX - 1);
    });
    assert_refused(file, "Stripe 1 extent overflows");
}

#[test]
fn test_footer_longer_than_the_file_is_refused() {
    let file = two_stripes().render_with(|_, postscript| {
        postscript.footer_length = Some(1 << 20);
    });
    assert_refused(file, "extend past the end of the file");
}

#[test]
fn test_metadata_longer_than_the_file_is_refused() {
    let file = two_stripes().render_with(|_, postscript| {
        postscript.metadata_length = Some(1 << 20);
    });
    assert_refused(file, "extend past the end of the file");
}

#[test]
fn test_postscript_longer_than_the_file_is_refused() {
    let mut file = two_stripes().render().to_vec();
    *file.last_mut().unwrap() = 250;
    assert_refused(Bytes::from(file), "too small");
}

fn zlib_file(block_size: Option<u64>) -> OrcFile {
    let mut file = OrcFile::new(vec![]);
    file.compression = CompressionKind::Zlib;
    file.block_size = block_size;
    file
}

#[test]
fn test_compression_block_sizes_within_the_chunk_header_range_are_read() {
    for block_size in [None, Some(1), Some(262_144), Some((1 << 23) - 1)] {
        let file = zlib_file(block_size).render();
        assert!(read(file, 8192).unwrap().is_empty(), "{block_size:?}");
    }
}

#[test]
fn test_block_size_is_ignored_without_compression() {
    for block_size in [Some(0), Some(1 << 30)] {
        let mut file = OrcFile::new(vec![numbered_rows(0, 5)]);
        file.block_size = block_size;
        assert_reads(file.render(), expected(0, 5));
    }
}

#[test]
#[ignore = "the compression block size is not validated yet"]
fn test_compression_block_size_zero_is_refused() {
    let error = read(zlib_file(Some(0)).render(), 8192).unwrap_err();
    assert!(
        error.contains("Compression block size 0 is outside"),
        "{error}"
    );
}

#[test]
#[ignore = "the compression block size is not validated yet"]
fn test_compression_block_size_beyond_the_chunk_header_range_is_refused() {
    for block_size in [1 << 23, 1 << 40, u64::MAX] {
        let error = read(zlib_file(Some(block_size)).render(), 8192).unwrap_err();
        assert!(error.contains("is outside"), "{block_size}: {error}");
    }
}

#[test]
#[ignore = "the compression kind is not validated yet"]
fn test_unknown_compression_kind_is_refused() {
    let file = OrcFile::new(vec![numbered_rows(0, 5)]).render_with(|_, postscript| {
        postscript.compression = Some(9);
    });
    let error = read(file, 8192).unwrap_err();
    assert!(error.contains("Unknown compression kind 9"), "{error}");
}

#[test]
#[ignore = "the column count is not compared with the type tree yet"]
fn test_stripe_footer_with_fewer_encodings_than_columns_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.columns.pop();
    assert_refused(file.render(), "2 column encodings for 3 columns");
}

#[test]
#[ignore = "the column count is not compared with the type tree yet"]
fn test_stripe_footer_with_more_encodings_than_columns_is_refused() {
    let mut file = two_stripes();
    file.stripes[1]
        .footer
        .columns
        .push(encoding(EncodingKind::Direct, None));
    assert_refused(file.render(), "4 column encodings for 3 columns");
}

fn boolean_file(boolean_encoding: ColumnEncoding) -> OrcFile {
    let mut file = OrcFile::new(vec![stripe(
        5,
        &[(1, Kind::Data, vec![0xFF, 0b1010_1000])],
        &[encoding(EncodingKind::Direct, None), boolean_encoding],
    )]);
    file.types = vec![
        node(TypeKind::Struct, &[1], &["b"]),
        node(TypeKind::Boolean, &[], &[]),
    ];
    file
}

#[test]
fn test_boolean_column_reads() {
    let batches = read(
        boolean_file(encoding(EncodingKind::Direct, None)).render(),
        8192,
    )
    .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
}

#[test]
#[ignore = "encoding kinds are not checked against the column type yet"]
fn test_dictionary_encoding_on_a_boolean_column_is_refused() {
    for kind in [EncodingKind::Dictionary, EncodingKind::DictionaryV2] {
        let file = boolean_file(encoding(kind, Some(2))).render();
        let error = read(file, 8192).unwrap_err();
        assert!(
            error.contains("column 1 is not a string column but has"),
            "{kind:?}: {error}"
        );
    }
}

#[test]
#[ignore = "encoding kinds are not checked against the column type yet"]
fn test_dictionary_encoding_on_an_int_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.columns[1] = encoding(EncodingKind::DictionaryV2, Some(2));
    assert_refused(file.render(), "column 1 is not a string column");
}

fn dictionary_stripe(
    dictionary_size: u32,
    lengths: &[u64],
    dictionary: &[u8],
    indexes: &[u64],
) -> StripeSpec {
    stripe(
        indexes.len() as u64,
        &[
            (1, Kind::Data, signed_run(&vec![7; indexes.len()])),
            (2, Kind::DictionaryData, dictionary.to_vec()),
            (2, Kind::Length, literal_run(lengths)),
            (2, Kind::Data, literal_run(indexes)),
        ],
        &[
            encoding(EncodingKind::Direct, None),
            encoding(EncodingKind::Direct, None),
            encoding(EncodingKind::Dictionary, Some(dictionary_size)),
        ],
    )
}

fn dictionary_values(file: Bytes) -> Result<Vec<String>, String> {
    let batches = read(file, 8192)?;
    let mut out = vec![];
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(1), &arrow::datatypes::DataType::Utf8)
            .map_err(|error| error.to_string())?;
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(column.iter().map(|value| value.unwrap().to_string()));
    }
    Ok(out)
}

#[test]
fn test_dictionary_column_reads() {
    let file = OrcFile::new(vec![dictionary_stripe(
        2,
        &[1, 2],
        b"xyz",
        &[0, 1, 1, 0, 1],
    )]);
    assert_eq!(
        dictionary_values(file.render()).unwrap(),
        ["x", "yz", "yz", "x", "yz"]
    );
}

#[test]
#[ignore = "the dictionary size is not compared with the stripe yet"]
fn test_dictionary_larger_than_the_stripe_is_refused() {
    let file = OrcFile::new(vec![dictionary_stripe(
        6,
        &[1, 2],
        b"xyz",
        &[0, 1, 1, 0, 1],
    )]);
    let error = dictionary_values(file.render()).unwrap_err();
    assert!(
        error.contains("dictionary of 6 entries for 5 rows"),
        "{error}"
    );
}

#[test]
#[ignore = "the dictionary size is not compared with its lengths yet"]
fn test_dictionary_size_below_the_length_entries_is_refused() {
    let file = OrcFile::new(vec![dictionary_stripe(
        1,
        &[1, 2],
        b"xyz",
        &[0, 0, 0, 0, 0],
    )]);
    let error = dictionary_values(file.render()).unwrap_err();
    assert!(
        error.contains("holds more entries than the dictionary size"),
        "{error}"
    );
}

#[test]
#[ignore = "the dictionary size is not compared with its data yet"]
fn test_dictionary_data_beyond_the_lengths_is_refused() {
    let file = OrcFile::new(vec![dictionary_stripe(
        2,
        &[1, 2],
        b"xyzzz",
        &[0, 1, 1, 0, 1],
    )]);
    let error = dictionary_values(file.render()).unwrap_err();
    assert!(error.contains("bytes beyond its lengths"), "{error}");
}

fn stride_file(stride: Option<u32>, entries: usize, bloom_filters: usize) -> OrcFile {
    let entry = RowIndexEntry {
        positions: vec![0, 0],
        statistics: Some(ColumnStatistics {
            number_of_values: Some(2),
            has_null: Some(false),
            int_statistics: Some(IntegerStatistics {
                minimum: Some(0),
                maximum: Some(2),
                sum: Some(2),
            }),
            ..Default::default()
        }),
    };
    let row_index = RowIndex {
        entry: vec![entry; entries],
    }
    .encode_to_vec();
    let bloom_filter = BloomFilter {
        num_hash_functions: Some(3),
        bitset: vec![0; 2],
        ..Default::default()
    };
    let bloom = BloomFilterIndex {
        bloom_filter: vec![bloom_filter; bloom_filters],
    }
    .encode_to_vec();
    let mut streams = vec![(1, Kind::RowIndex, row_index)];
    if bloom_filters > 0 {
        streams.push((1, Kind::BloomFilter, bloom));
    }
    streams.extend([
        (1, Kind::Data, signed_run(&[0, 1, 2, 3, 4])),
        (2, Kind::Length, literal_run(&[1; 5])),
        (2, Kind::Data, b"abcde".to_vec()),
    ]);
    let mut file = OrcFile::new(vec![stripe(5, &streams, &direct(3))]);
    file.row_index_stride = stride;
    file
}

fn row_indexes(file: &OrcFile) -> Result<usize, String> {
    let bytes = file.render();
    let builder = ArrowReaderBuilder::try_new(bytes.clone()).map_err(|error| error.to_string())?;
    let metadata = builder.file_metadata();
    let mut reader = bytes;
    let stripe = Stripe::new(
        &mut reader,
        metadata,
        metadata.root_data_type(),
        &metadata.stripe_metadatas()[0],
    )
    .map_err(|error| error.to_string())?;
    let index = stripe
        .read_row_indexes(metadata)
        .map_err(|error| error.to_string())?;
    Ok(index.column(1).unwrap().num_row_groups())
}

#[test]
fn test_row_index_with_one_entry_per_stride_reads() {
    assert_eq!(row_indexes(&stride_file(Some(2), 3, 0)), Ok(3));
    assert_eq!(row_indexes(&stride_file(Some(5), 1, 0)), Ok(1));
    assert_eq!(row_indexes(&stride_file(Some(10_000), 1, 0)), Ok(1));
}

#[test]
#[ignore = "row index entries are not compared with the stride yet"]
fn test_row_index_with_too_few_entries_is_refused() {
    let error = row_indexes(&stride_file(Some(2), 2, 0)).unwrap_err();
    assert!(
        error.contains("row index of column 1 has 2 entries for 5 rows at a stride of 2"),
        "{error}"
    );
}

#[test]
#[ignore = "row index entries are not compared with the stride yet"]
fn test_row_index_with_too_many_entries_is_refused() {
    let error = row_indexes(&stride_file(Some(2), 4, 0)).unwrap_err();
    assert!(
        error.contains("row index of column 1 has 4 entries for 5 rows at a stride of 2"),
        "{error}"
    );
}

#[test]
#[ignore = "row index entries are not compared with the stride yet"]
fn test_row_index_in_a_file_with_stride_zero_is_refused() {
    let error = row_indexes(&stride_file(Some(0), 1, 0)).unwrap_err();
    assert!(error.contains("stride is 0"), "{error}");
}

#[test]
fn test_bloom_filters_matching_the_row_index_entries_read() {
    assert_eq!(row_indexes(&stride_file(Some(2), 3, 3)), Ok(3));
}

#[test]
#[ignore = "bloom filter counts are asserted rather than refused"]
fn test_bloom_filters_fewer_than_row_index_entries_are_refused() {
    let error = row_indexes(&stride_file(Some(2), 3, 2)).unwrap_err();
    assert!(
        error.contains("2 bloom filters for 3 row index entries"),
        "{error}"
    );
}

#[cfg(feature = "async")]
#[tokio::test]
#[ignore = "stream lengths are not compared with the stripe lengths yet"]
async fn test_async_reader_refuses_stream_lengths_that_do_not_add_up() {
    use futures_util::TryStreamExt;

    let mut file = two_stripes();
    let length = file.stripes[1].footer.streams[0].length();
    file.stripes[1].footer.streams[0].length = Some(length + 1);
    let reader = ArrowReaderBuilder::try_new_async(std::io::Cursor::new(file.render()))
        .await
        .unwrap()
        .build_async();
    let error = reader.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("stripe footer streams hold"),
        "{error}"
    );
}
