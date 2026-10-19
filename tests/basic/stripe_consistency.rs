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
    BloomFilter, BloomFilterIndex, BucketStatistics, ColumnEncoding, ColumnStatistics,
    CompressionKind, Encryption, Footer, IntegerStatistics, Metadata, PostScript, RowIndex,
    RowIndexEntry, Stream, StringStatistics, StripeFooter, StripeInformation, StripeStatistics,
    Type,
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

type RawEdit = fn(Vec<u8>) -> Vec<u8>;

type Append = (Option<(u64, usize)>, Vec<u8>);

fn apply_appends(mut bytes: Vec<u8>, appends: &[Append]) -> Vec<u8> {
    for (target, extra) in appends {
        bytes = match target {
            Some((number, occurrence)) => append_to_field(&bytes, *number, *occurrence, extra),
            None => [bytes, extra.clone()].concat(),
        };
    }
    bytes
}

struct StripeSpec {
    rows: u64,
    index: u64,
    data: u64,
    body: Vec<u8>,
    footer: StripeFooter,
    footer_edit: Option<RawEdit>,
    footer_appends: Vec<Append>,
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
            Kind::RowIndex | Kind::BloomFilter | Kind::BloomFilterUtf8 | Kind::EncryptedIndex
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
        footer_edit: None,
        footer_appends: vec![],
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
    stripe_padding: usize,
    trailing: Vec<u8>,
    stripe_statistics: Vec<StripeStatistics>,
    footer_edit: Option<RawEdit>,
    postscript_edit: Option<RawEdit>,
    footer_appends: Vec<Append>,
    postscript_append: Vec<u8>,
}

impl OrcFile {
    fn new(stripes: Vec<StripeSpec>) -> Self {
        Self {
            types: int_and_string(),
            stripes,
            row_index_stride: None,
            compression: CompressionKind::None,
            block_size: None,
            stripe_padding: 0,
            trailing: vec![],
            stripe_statistics: vec![],
            footer_edit: None,
            postscript_edit: None,
            footer_appends: vec![],
            postscript_append: vec![],
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
        for (index, spec) in self.stripes.iter().enumerate() {
            if index > 0 {
                file.resize(file.len() + self.stripe_padding, 0);
            }
            let offset = file.len() as u64;
            file.extend_from_slice(&spec.body);
            let mut stripe_footer = spec.footer.encode_to_vec();
            if let Some(edit) = spec.footer_edit {
                stripe_footer = edit(stripe_footer);
            }
            stripe_footer = apply_appends(stripe_footer, &spec.footer_appends);
            let stripe_footer = self.section(stripe_footer);
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
        file.extend_from_slice(&self.trailing);
        footer.content_length = Some(file.len() as u64 - MAGIC_LENGTH);
        footer.number_of_rows = Some(self.stripes.iter().map(|spec| spec.rows).sum());
        let metadata = self.section(
            Metadata {
                stripe_stats: self.stripe_statistics.clone(),
            }
            .encode_to_vec(),
        );
        file.extend_from_slice(&metadata);
        let mut postscript = PostScript {
            compression: Some(self.compression as i32),
            compression_block_size: self.block_size,
            version: vec![0, 12],
            metadata_length: Some(metadata.len() as u64),
            magic: Some("ORC".to_string()),
            ..Default::default()
        };
        edit(&mut footer, &mut postscript);
        let mut footer_bytes = footer.encode_to_vec();
        if let Some(edit) = self.footer_edit {
            footer_bytes = edit(footer_bytes);
        }
        footer_bytes = apply_appends(footer_bytes, &self.footer_appends);
        let footer = self.section(footer_bytes);
        postscript.footer_length = postscript.footer_length.or(Some(footer.len() as u64));
        file.extend_from_slice(&footer);
        let mut postscript = postscript.encode_to_vec();
        if let Some(edit) = self.postscript_edit {
            postscript = edit(postscript);
        }
        postscript.extend_from_slice(&self.postscript_append);
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
fn test_stream_longer_than_declared_is_refused() {
    let mut file = two_stripes();
    let length = file.stripes[0].footer.streams[0].length();
    file.stripes[0].footer.streams[0].length = Some(length + 1);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
fn test_stream_shorter_than_declared_is_refused() {
    let mut file = two_stripes();
    let length = file.stripes[0].footer.streams[2].length();
    file.stripes[0].footer.streams[2].length = Some(length - 1);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
fn test_index_and_data_lengths_that_swap_bytes_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        let stripe = &mut footer.stripes[0];
        stripe.index_length = Some(stripe.index_length() + 1);
        stripe.data_length = Some(stripe.data_length() - 1);
    });
    assert_refused(file, "stripe footer streams hold");
}

#[test]
fn test_stream_length_zeroed_in_the_stripe_footer_is_refused() {
    let mut file = two_stripes();
    file.stripes[1].footer.streams[0].length = Some(0);
    assert_refused(file.render(), "stripe footer streams hold");
}

#[test]
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
fn test_length_stream_on_an_int_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(Kind::Length as i32);
    assert_refused(
        file.render(),
        "column 1 of type Some(Int) cannot have a Length stream",
    );
}

#[test]
fn test_secondary_stream_on_a_string_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[1].kind = Some(Kind::Secondary as i32);
    assert_refused(
        file.render(),
        "column 2 of type Some(String) cannot have a Secondary stream",
    );
}

#[test]
fn test_data_stream_on_a_struct_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].column = Some(0);
    assert_refused(
        file.render(),
        "column 0 of type Some(Struct) cannot have a Data stream",
    );
}

#[test]
fn test_dictionary_data_stream_on_an_int_column_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(Kind::DictionaryData as i32);
    assert_refused(
        file.render(),
        "column 1 of type Some(Int) cannot have a DictionaryData stream",
    );
}

#[test]
fn test_stream_of_a_column_beyond_the_type_tree_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].column = Some(9);
    assert_refused(
        file.render(),
        "column 9 of type None cannot have a Data stream",
    );
}

#[test]
fn test_stream_of_unknown_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = Some(77);
    assert_refused(file.render(), "column 1 has a stream of no known kind");
}

#[test]
fn test_encoding_of_unknown_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.columns[1].kind = Some(9);
    assert_refused(file.render(), "column 1 has no known encoding kind");
}

#[test]
fn test_footer_row_count_above_the_stripes_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.number_of_rows = Some(footer.number_of_rows() + 1);
    });
    assert_refused(file, "Footer states 10 rows but its stripes hold 9");
}

#[test]
fn test_footer_row_count_below_the_stripes_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.number_of_rows = Some(footer.number_of_rows() - 1);
    });
    assert_refused(file, "Footer states 8 rows but its stripes hold 9");
}

#[test]
fn test_stripe_row_count_contradicting_the_footer_total_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].number_of_rows = Some(5);
    });
    assert_refused(file, "Footer states 9 rows but its stripes hold 10");
}

#[test]
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
fn test_stripe_ending_beyond_the_file_tail_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].data_length = Some(footer.stripes[1].data_length() + 1000);
    });
    assert_refused(file, "Stripe 1 ends at");
}

#[test]
fn test_overlapping_stripes_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].offset = Some(footer.stripes[0].offset() + 1);
    });
    assert_refused(file, "Stripe 1 starts at");
}

#[test]
fn test_stripes_listed_out_of_order_are_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes.swap(0, 1);
    });
    assert_refused(file, "Stripe 1 starts at");
}

#[test]
fn test_stripe_starting_inside_the_magic_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[0].offset = Some(0);
    });
    assert_refused(file, "Stripe 0 starts at 0");
}

#[test]
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
fn test_compression_block_size_zero_is_refused() {
    let error = read(zlib_file(Some(0)).render(), 8192).unwrap_err();
    assert!(
        error.contains("Compression block size 0 is outside"),
        "{error}"
    );
}

#[test]
fn test_compression_block_size_beyond_the_chunk_header_range_is_refused() {
    for block_size in [1 << 23, 1 << 40, u64::MAX] {
        let error = read(zlib_file(Some(block_size)).render(), 8192).unwrap_err();
        assert!(error.contains("is outside"), "{block_size}: {error}");
    }
}

#[test]
fn test_unknown_compression_kind_is_refused() {
    let file = OrcFile::new(vec![numbered_rows(0, 5)]).render_with(|_, postscript| {
        postscript.compression = Some(9);
    });
    let error = read(file, 8192).unwrap_err();
    assert!(error.contains("Unknown compression kind 9"), "{error}");
}

#[test]
fn test_stripe_footer_with_fewer_encodings_than_columns_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.columns.pop();
    assert_refused(file.render(), "2 column encodings for 3 columns");
}

#[test]
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
fn test_row_index_with_too_few_entries_is_refused() {
    let error = row_indexes(&stride_file(Some(2), 2, 0)).unwrap_err();
    assert!(
        error.contains("row index of column 1 has 2 entries for 5 rows at a stride of 2"),
        "{error}"
    );
}

#[test]
fn test_row_index_with_too_many_entries_is_refused() {
    let error = row_indexes(&stride_file(Some(2), 4, 0)).unwrap_err();
    assert!(
        error.contains("row index of column 1 has 4 entries for 5 rows at a stride of 2"),
        "{error}"
    );
}

#[test]
fn test_row_index_in_a_file_with_stride_zero_is_refused() {
    let error = row_indexes(&stride_file(Some(0), 1, 0)).unwrap_err();
    assert!(error.contains("stride is 0"), "{error}");
}

#[test]
fn test_bloom_filters_matching_the_row_index_entries_read() {
    assert_eq!(row_indexes(&stride_file(Some(2), 3, 3)), Ok(3));
}

#[test]
fn test_bloom_filters_fewer_than_row_index_entries_are_refused() {
    let error = row_indexes(&stride_file(Some(2), 3, 2)).unwrap_err();
    assert!(
        error.contains("2 bloom filters for 3 row index entries"),
        "{error}"
    );
}

#[cfg(feature = "async")]
#[tokio::test]
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

fn typed_statistics() -> Vec<ColumnStatistics> {
    vec![
        ColumnStatistics {
            number_of_values: Some(5),
            ..Default::default()
        },
        ColumnStatistics {
            number_of_values: Some(5),
            int_statistics: Some(IntegerStatistics {
                minimum: Some(0),
                maximum: Some(4),
                sum: Some(10),
            }),
            ..Default::default()
        },
        ColumnStatistics {
            number_of_values: Some(5),
            string_statistics: Some(StringStatistics::default()),
            ..Default::default()
        },
    ]
}

fn statistics_file(statistics: Vec<ColumnStatistics>) -> Bytes {
    OrcFile::new(vec![numbered_rows(0, 5)]).render_with(|footer, _| {
        footer.statistics = statistics;
    })
}

fn bucket_statistics() -> ColumnStatistics {
    ColumnStatistics {
        number_of_values: Some(5),
        bucket_statistics: Some(BucketStatistics { count: vec![2] }),
        ..Default::default()
    }
}

#[test]
fn test_statistics_of_the_column_types_read() {
    assert_reads(statistics_file(typed_statistics()), expected(0, 5));
}

#[test]
fn test_stripe_with_a_gap_before_it_reads() {
    let mut file = two_stripes();
    file.stripe_padding = 17;
    assert_reads(file.render(), expected(0, 9));
}

#[test]
fn test_gap_after_the_last_stripe_of_an_encrypted_file_reads() {
    let mut file = two_stripes();
    file.trailing = vec![0; 9];
    assert_reads(
        file.render_with(|footer, _| footer.encryption = Some(Encryption::default())),
        expected(0, 9),
    );
}

#[test]
fn test_statistics_of_another_column_type_are_refused() {
    let mut statistics = typed_statistics();
    statistics.swap(1, 2);
    assert_refused(
        statistics_file(statistics),
        "column 1 of type Int has statistics of another type",
    );
}

#[test]
fn test_bucket_statistics_on_an_int_column_are_refused() {
    let mut statistics = typed_statistics();
    statistics[1] = bucket_statistics();
    assert_refused(
        statistics_file(statistics),
        "has statistics of another type",
    );
}

#[test]
fn test_integer_statistics_on_the_struct_column_are_refused() {
    let mut statistics = typed_statistics();
    statistics[0] = statistics[1].clone();
    assert_refused(
        statistics_file(statistics),
        "column 0 of type Struct has statistics of another type",
    );
}

#[test]
fn test_statistics_that_are_not_one_per_column_are_refused() {
    let mut statistics = typed_statistics();
    statistics.pop();
    assert_refused(
        statistics_file(statistics),
        "2 column statistics for 3 columns",
    );
}

#[test]
fn test_stripe_statistics_of_another_column_type_are_refused() {
    let mut file = OrcFile::new(vec![numbered_rows(0, 5)]);
    let mut statistics = typed_statistics();
    statistics.swap(1, 2);
    file.stripe_statistics = vec![StripeStatistics {
        col_stats: statistics,
    }];
    assert_refused(file.render(), "has statistics of another type");
}

#[test]
fn test_stripe_statistics_of_the_column_types_read() {
    let mut file = OrcFile::new(vec![numbered_rows(0, 5)]);
    file.stripe_statistics = vec![StripeStatistics {
        col_stats: typed_statistics(),
    }];
    assert_reads(file.render(), expected(0, 5));
}

#[test]
fn test_bytes_between_the_last_stripe_and_the_file_tail_are_refused() {
    let mut file = two_stripes();
    file.trailing = vec![0; 5];
    assert_refused(file.render(), "The last stripe ends at");
}

#[test]
fn test_bytes_after_the_magic_of_a_file_without_stripes_are_refused() {
    let mut file = OrcFile::new(vec![]);
    file.trailing = vec![0; 5];
    assert_refused(file.render(), "The last stripe ends at 3");
}

#[test]
fn test_last_stripe_ending_before_the_file_tail_is_refused() {
    let file = two_stripes().render_with(|footer, _| {
        footer.stripes[1].footer_length = Some(footer.stripes[1].footer_length() - 1);
    });
    assert_refused(file, "The last stripe ends at");
}

#[test]
fn test_type_without_a_kind_is_refused() {
    let mut file = two_stripes();
    file.types[1].kind = None;
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("column index 1 has no known kind"),
        "{error}"
    );
}

#[test]
fn test_type_of_unknown_kind_is_refused() {
    let mut file = two_stripes();
    file.types[2].kind = Some(99);
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("column index 2 has no known kind"),
        "{error}"
    );
}

#[test]
fn test_stream_without_a_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer.streams[0].kind = None;
    assert_refused(file.render(), "column 1 has a stream of no known kind");
}

#[test]
fn test_encoding_without_a_kind_is_refused() {
    let mut file = two_stripes();
    file.stripes[1].footer.columns[2].kind = None;
    assert_refused(file.render(), "column 2 has no known encoding kind");
}

#[test]
fn test_encrypted_streams_on_any_column_are_not_refused() {
    let encrypted = stripe(
        5,
        &[
            (2, Kind::EncryptedIndex, vec![0; 3]),
            (1, Kind::Data, signed_run(&[0, 1, 2, 3, 4])),
            (2, Kind::Length, literal_run(&[1; 5])),
            (2, Kind::Data, b"abcde".to_vec()),
            (0, Kind::EncryptedData, vec![0; 4]),
        ],
        &direct(3),
    );
    assert_reads(OrcFile::new(vec![encrypted]).render(), expected(0, 5));
}

fn field_varint(number: u64, value: u64) -> Vec<u8> {
    let mut out = varint(number << 3);
    out.extend(varint(value));
    out
}

fn field_bytes(number: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = varint((number << 3) | 2);
    out.extend(varint(bytes.len() as u64));
    out.extend_from_slice(bytes);
    out
}

fn read_varint(buf: &[u8], at: &mut usize) -> u64 {
    let mut value = 0;
    let mut shift = 0;
    loop {
        let byte = buf[*at];
        *at += 1;
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

fn append_to_field(buf: &[u8], number: u64, occurrence: usize, extra: &[u8]) -> Vec<u8> {
    let mut out = vec![];
    let mut at = 0;
    let mut seen = 0;
    while at < buf.len() {
        let start = at;
        let tag = read_varint(buf, &mut at);
        match tag & 7 {
            0 => {
                read_varint(buf, &mut at);
                out.extend_from_slice(&buf[start..at]);
            }
            2 => {
                let length = read_varint(buf, &mut at) as usize;
                let body = &buf[at..at + length];
                at += length;
                if tag >> 3 == number {
                    seen += 1;
                    if seen == occurrence + 1 {
                        let mut body = body.to_vec();
                        body.extend_from_slice(extra);
                        out.extend(field_bytes(number, &body));
                        continue;
                    }
                }
                out.extend_from_slice(&buf[start..at]);
            }
            wire => panic!("unexpected wire type {wire}"),
        }
    }
    assert!(seen > occurrence, "field {number} occurs {seen} times");
    out
}

#[test]
fn test_unpacked_repeated_fields_read() {
    let mut file = OrcFile::new(vec![numbered_rows(0, 5)]);
    file.types[0].subtypes.clear();
    file.types[0].field_names.clear();
    file.footer_edit = Some(|footer| {
        let extra = [
            field_varint(2, 1),
            field_varint(2, 2),
            field_bytes(3, b"a"),
            field_bytes(3, b"s"),
        ]
        .concat();
        append_to_field(&footer, 4, 0, &extra)
    });
    assert_reads(file.render(), expected(0, 5));
}

#[test]
fn test_footer_field_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.footer_edit = Some(|footer| [footer, field_varint(6, 9)].concat());
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("field 6 of the footer occurs more than once"),
        "{error}"
    );
}

#[test]
fn test_type_kind_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.footer_edit = Some(|footer| append_to_field(&footer, 4, 1, &field_varint(1, 0)));
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("field 1 of a type occurs more than once"),
        "{error}"
    );
}

#[test]
fn test_stripe_information_field_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.footer_edit = Some(|footer| append_to_field(&footer, 3, 1, &field_varint(5, 1)));
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("field 5 of a stripe information occurs more than once"),
        "{error}"
    );
}

#[test]
fn test_postscript_field_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.postscript_edit = Some(|postscript| [postscript, field_varint(2, 0)].concat());
    let error = read(file.render(), 8192).unwrap_err();
    assert!(
        error.contains("field 2 of the postscript occurs more than once"),
        "{error}"
    );
}

#[test]
fn test_stream_field_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer_edit =
        Some(|footer| append_to_field(&footer, 1, 2, &field_varint(3, 0)));
    assert_refused(file.render(), "field 3 of a stream occurs more than once");
}

#[test]
fn test_column_encoding_field_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.stripes[1].footer_edit =
        Some(|footer| append_to_field(&footer, 2, 1, &field_varint(1, 0)));
    assert_refused(
        file.render(),
        "field 1 of a column encoding occurs more than once",
    );
}

#[test]
fn test_stripe_footer_timezone_that_occurs_twice_is_refused() {
    let mut file = two_stripes();
    file.stripes[0].footer_edit =
        Some(|footer| [footer, field_bytes(3, b"UTC"), field_bytes(3, b"UTC")].concat());
    assert_refused(
        file.render(),
        "field 3 of a stripe footer occurs more than once",
    );
}

#[test]
fn test_dictionary_encoding_without_a_size_is_refused() {
    let mut stripe = dictionary_stripe(2, &[1, 2], b"xyz", &[0, 1, 1, 0, 1]);
    stripe.footer.columns[2].dictionary_size = None;
    let error = dictionary_values(OrcFile::new(vec![stripe]).render()).unwrap_err();
    assert!(error.contains("column 2 has no dictionary size"), "{error}");
}

#[test]
fn test_dictionary_data_under_a_direct_encoding_is_refused() {
    let mut stripe = dictionary_stripe(2, &[1, 2], b"xyz", &[0, 1, 1, 0, 1]);
    stripe.footer.columns[2] = encoding(EncodingKind::Direct, None);
    let error = dictionary_values(OrcFile::new(vec![stripe]).render()).unwrap_err();
    assert!(
        error.contains("column 2 has a DictionaryData stream but Direct encoding"),
        "{error}"
    );
}

fn tinyint_file(minimum: i64, maximum: i64) -> Bytes {
    let mut file = OrcFile::new(vec![stripe(
        5,
        &[(1, Kind::Data, vec![0xFB, 1, 2, 3, 4, 5])],
        &direct(2),
    )]);
    file.types = vec![
        node(TypeKind::Struct, &[1], &["b"]),
        node(TypeKind::Byte, &[], &[]),
    ];
    file.render_with(|footer, _| {
        footer.statistics = vec![
            ColumnStatistics {
                number_of_values: Some(5),
                ..Default::default()
            },
            ColumnStatistics {
                number_of_values: Some(5),
                int_statistics: Some(IntegerStatistics {
                    minimum: Some(minimum),
                    maximum: Some(maximum),
                    sum: None,
                }),
                ..Default::default()
            },
        ];
    })
}

#[test]
fn test_integer_statistics_within_the_column_type_read() {
    let batches = read(tinyint_file(-128, 127), 8192).unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
}

#[test]
fn test_integer_statistics_beyond_the_column_type_are_refused() {
    for (minimum, maximum) in [(-129, 5), (0, 128), (i64::MIN, 5), (0, i64::MAX)] {
        let error = read(tinyint_file(minimum, maximum), 8192).unwrap_err();
        assert!(
            error.contains("column 1 of type Byte has statistics outside the type's range"),
            "{minimum}..{maximum}: {error}"
        );
    }
}

#[test]
fn test_short_and_int_statistics_beyond_the_column_type_are_refused() {
    let file = |kind: TypeKind, minimum: i64, maximum: i64| {
        let mut file = OrcFile::new(vec![numbered_rows(0, 5)]);
        file.types[1].kind = Some(kind as i32);
        file.render_with(|footer, _| {
            footer.statistics = typed_statistics();
            let stats = footer.statistics[1].int_statistics.as_mut().unwrap();
            stats.minimum = Some(minimum);
            stats.maximum = Some(maximum);
        })
    };
    assert!(read(file(TypeKind::Short, -32768, 32767), 8192).is_ok());
    assert!(read(file(TypeKind::Int, i32::MIN.into(), i32::MAX.into()), 8192).is_ok());
    assert!(read(file(TypeKind::Long, i64::MIN, i64::MAX), 8192).is_ok());
    let error = read(file(TypeKind::Short, 0, 32768), 8192).unwrap_err();
    assert!(
        error.contains("type Short has statistics outside"),
        "{error}"
    );
    let error = read(file(TypeKind::Int, i64::from(i32::MIN) - 1, 0), 8192).unwrap_err();
    assert!(error.contains("type Int has statistics outside"), "{error}");
}

#[test]
fn test_dictionary_with_one_entry_per_row_reads() {
    let file = OrcFile::new(vec![dictionary_stripe(
        5,
        &[1, 1, 1, 1, 1],
        b"vwxyz",
        &[4, 3, 2, 1, 0],
    )]);
    assert_eq!(
        dictionary_values(file.render()).unwrap(),
        ["z", "y", "x", "w", "v"]
    );
}

#[test]
fn test_dictionary_encoding_on_varchar_and_char_columns_reads() {
    for kind in [TypeKind::Varchar, TypeKind::Char] {
        let mut file = OrcFile::new(vec![dictionary_stripe(
            2,
            &[1, 2],
            b"xyz",
            &[0, 1, 1, 0, 1],
        )]);
        file.types[2].kind = Some(kind as i32);
        assert_eq!(
            dictionary_values(file.render()).unwrap(),
            ["x", "yz", "yz", "x", "yz"],
            "{kind:?}"
        );
    }
}

fn all_type_kinds() -> Vec<TypeKind> {
    vec![
        TypeKind::Boolean,
        TypeKind::Byte,
        TypeKind::Short,
        TypeKind::Int,
        TypeKind::Long,
        TypeKind::Float,
        TypeKind::Double,
        TypeKind::String,
        TypeKind::Binary,
        TypeKind::Timestamp,
        TypeKind::TimestampInstant,
        TypeKind::Decimal,
        TypeKind::Date,
        TypeKind::Varchar,
        TypeKind::Char,
        TypeKind::Struct,
        TypeKind::List,
        TypeKind::Map,
        TypeKind::Union,
    ]
}

fn tree_with(kind: TypeKind) -> Vec<Type> {
    let leaf = || node(TypeKind::Int, &[], &[]);
    match kind {
        TypeKind::Struct => vec![
            node(TypeKind::Struct, &[1], &["c"]),
            node(kind, &[2], &["x"]),
            leaf(),
        ],
        TypeKind::List => vec![
            node(TypeKind::Struct, &[1], &["c"]),
            node(kind, &[2], &[]),
            leaf(),
        ],
        TypeKind::Map => vec![
            node(TypeKind::Struct, &[1], &["c"]),
            node(kind, &[2, 3], &[]),
            leaf(),
            leaf(),
        ],
        TypeKind::Union => vec![
            node(TypeKind::Struct, &[1], &["c"]),
            node(kind, &[2], &[]),
            leaf(),
        ],
        _ => vec![node(TypeKind::Struct, &[1], &["c"]), node(kind, &[], &[])],
    }
}

fn stripe_validates(file: &OrcFile) -> Result<(), String> {
    let bytes = file.render();
    let builder = ArrowReaderBuilder::try_new(bytes.clone()).map_err(|error| error.to_string())?;
    let metadata = builder.file_metadata();
    let mut reader = bytes;
    Stripe::new(
        &mut reader,
        metadata,
        metadata.root_data_type(),
        &metadata.stripe_metadatas()[0],
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

fn single_stream_file(kind: TypeKind, stream_kind: Kind) -> OrcFile {
    let types = tree_with(kind);
    let columns = types.len();
    let mut file = OrcFile::new(vec![stripe(
        0,
        &[(1, stream_kind, vec![0; 3])],
        &direct(columns),
    )]);
    file.types = types;
    file
}

fn stream_kind_fits(kind: TypeKind, stream_kind: Kind) -> bool {
    use TypeKind::*;
    match stream_kind {
        Kind::Present
        | Kind::RowIndex
        | Kind::BloomFilter
        | Kind::BloomFilterUtf8
        | Kind::EncryptedIndex
        | Kind::EncryptedData => true,
        Kind::Data => !matches!(kind, Struct | List | Map),
        Kind::Length => matches!(kind, String | Varchar | Char | Binary | List | Map),
        Kind::DictionaryData | Kind::DictionaryCount => matches!(kind, String | Varchar | Char),
        Kind::Secondary => matches!(kind, Decimal | Timestamp | TimestampInstant),
        Kind::StripeStatistics | Kind::FileStatistics => false,
    }
}

#[test]
fn test_stream_kinds_a_column_type_can_have() {
    let stream_kinds = [
        Kind::Present,
        Kind::Data,
        Kind::Length,
        Kind::DictionaryData,
        Kind::DictionaryCount,
        Kind::Secondary,
        Kind::RowIndex,
        Kind::BloomFilter,
        Kind::BloomFilterUtf8,
        Kind::EncryptedIndex,
        Kind::EncryptedData,
        Kind::StripeStatistics,
        Kind::FileStatistics,
    ];
    for kind in all_type_kinds() {
        for stream_kind in stream_kinds {
            let dictionary_data = stream_kind == Kind::DictionaryData;
            let mut file = single_stream_file(kind, stream_kind);
            if dictionary_data {
                file.stripes[0].footer.columns[1] = encoding(EncodingKind::Dictionary, Some(0));
            }
            let result = stripe_validates(&file);
            assert_eq!(
                result.is_ok(),
                stream_kind_fits(kind, stream_kind),
                "{kind:?} {stream_kind:?}: {result:?}"
            );
        }
    }
}

#[test]
fn test_index_streams_count_toward_the_index_length() {
    for stream_kind in [
        Kind::RowIndex,
        Kind::BloomFilter,
        Kind::BloomFilterUtf8,
        Kind::EncryptedIndex,
    ] {
        let mut file = single_stream_file(TypeKind::Int, stream_kind);
        assert_eq!(stripe_validates(&file), Ok(()), "{stream_kind:?}");
        file.stripes[0].index = 0;
        file.stripes[0].data = 3;
        assert!(stripe_validates(&file).is_err(), "{stream_kind:?}");
    }
    for stream_kind in [Kind::Present, Kind::Data, Kind::EncryptedData] {
        let mut file = single_stream_file(TypeKind::Int, stream_kind);
        assert_eq!(stripe_validates(&file), Ok(()), "{stream_kind:?}");
        file.stripes[0].index = 3;
        file.stripes[0].data = 0;
        assert!(stripe_validates(&file).is_err(), "{stream_kind:?}");
    }
}

#[test]
fn test_encoding_kinds_a_column_type_can_have() {
    let kinds = [
        EncodingKind::Direct,
        EncodingKind::Dictionary,
        EncodingKind::DirectV2,
        EncodingKind::DictionaryV2,
    ];
    for kind in all_type_kinds() {
        for encoding_kind in kinds {
            let mut file = single_stream_file(kind, Kind::Present);
            file.stripes[0].footer.columns[1] = encoding(encoding_kind, Some(0));
            let dictionary = matches!(
                encoding_kind,
                EncodingKind::Dictionary | EncodingKind::DictionaryV2
            );
            let string = matches!(kind, TypeKind::String | TypeKind::Varchar | TypeKind::Char);
            let result = stripe_validates(&file);
            assert_eq!(
                result.is_ok(),
                !dictionary || string,
                "{kind:?} {encoding_kind:?}: {result:?}"
            );
        }
    }
}

#[test]
fn test_stream_of_a_utf8_bloom_filter_reads() {
    let bloom_filter = BloomFilter {
        num_hash_functions: Some(3),
        bitset: vec![0; 2],
        ..Default::default()
    };
    let bloom = BloomFilterIndex {
        bloom_filter: vec![bloom_filter; 3],
    }
    .encode_to_vec();
    let mut file = stride_file(Some(2), 3, 0);
    file.stripes[0] = stripe(
        5,
        &[
            (
                1,
                Kind::RowIndex,
                file.stripes[0].body[..file.stripes[0].index as usize].to_vec(),
            ),
            (1, Kind::BloomFilterUtf8, bloom),
            (1, Kind::Data, signed_run(&[0, 1, 2, 3, 4])),
            (2, Kind::Length, literal_run(&[1; 5])),
            (2, Kind::Data, b"abcde".to_vec()),
        ],
        &direct(3),
    );
    assert_eq!(row_indexes(&file), Ok(3));
}

#[test]
fn test_bloom_filters_more_than_row_index_entries_are_refused() {
    let error = row_indexes(&stride_file(Some(2), 3, 4)).unwrap_err();
    assert!(
        error.contains("4 bloom filters for 3 row index entries"),
        "{error}"
    );
}

#[test]
fn test_every_singular_field_of_the_metadata_messages_is_checked() {
    let twice = |number: u64, payload: Option<&[u8]>| {
        let one = match payload {
            Some(payload) => field_bytes(number, payload),
            None => field_varint(number, 0),
        };
        [one.clone(), one].concat()
    };
    let rejects = |file: OrcFile, message: &str| {
        let error = read(file.render(), 8192).unwrap_err();
        assert!(
            error.contains(&format!("{message} occurs more than once")),
            "{message}: {error}"
        );
    };

    for (number, payload) in [
        (1, None),
        (2, None),
        (3, None),
        (5, None),
        (6, None),
        (8000, Some(b"ORC".as_slice())),
    ] {
        let mut file = two_stripes();
        file.postscript_append = twice(number, payload);
        rejects(file, &format!("field {number} of the postscript"));
    }
    for (number, payload) in [
        (1, None),
        (2, None),
        (6, None),
        (8, None),
        (9, None),
        (10, Some(b"".as_slice())),
        (11, None),
        (12, Some(b"x".as_slice())),
    ] {
        let mut file = two_stripes();
        file.footer_appends = vec![(None, twice(number, payload))];
        rejects(file, &format!("field {number} of the footer"));
    }
    for number in 1..=6 {
        let mut file = two_stripes();
        file.footer_appends = vec![(Some((3, 0)), twice(number, None))];
        rejects(file, &format!("field {number} of a stripe information"));
    }
    for number in [1, 4, 5, 6] {
        let mut file = two_stripes();
        file.footer_appends = vec![(Some((4, 1)), twice(number, None))];
        rejects(file, &format!("field {number} of a type"));
    }
    for number in 1..=3 {
        let mut file = two_stripes();
        file.stripes[0].footer_appends = vec![(Some((1, 0)), twice(number, None))];
        rejects(file, &format!("field {number} of a stream"));
        let mut file = two_stripes();
        file.stripes[0].footer_appends = vec![(Some((2, 0)), twice(number, None))];
        rejects(file, &format!("field {number} of a column encoding"));
    }
    let mut file = two_stripes();
    file.stripes[0].footer_appends = vec![(None, twice(3, Some(b"x")))];
    rejects(file, "field 3 of a stripe footer");
}
