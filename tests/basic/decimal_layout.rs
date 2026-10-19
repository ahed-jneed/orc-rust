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

use std::fs::File;

use arrow::array::{Array, Decimal128Array};
use orc_rust::arrow_reader::ArrowReaderBuilder;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

fn read_all(
    name: &str,
    batch_size: usize,
) -> Result<Vec<Vec<Option<i128>>>, Box<dyn std::error::Error>> {
    let f = File::open(basic_path(&format!("decimal_layout_{name}.orc"))).unwrap();
    let reader = ArrowReaderBuilder::try_new(f)?
        .with_batch_size(batch_size)
        .build();
    let mut columns: Vec<Vec<Option<i128>>> = vec![];
    for batch in reader {
        let batch = batch?;
        for (i, column) in batch.columns().iter().enumerate() {
            if columns.len() <= i {
                columns.push(vec![]);
            }
            if let Some(d) = column.as_any().downcast_ref::<Decimal128Array>() {
                columns[i].extend(d.iter());
            }
        }
    }
    Ok(columns)
}

fn assert_refused(name: &str, message: &str) {
    for batch_size in [1, 8192] {
        let error = read_all(name, batch_size).expect_err(name).to_string();
        assert!(
            error.contains(message),
            "{name} batch {batch_size}: {error}"
        );
    }
}

fn assert_reads_legacy_values(name: &str) {
    for batch_size in [1, 8192] {
        let columns = read_all(name, batch_size).unwrap();
        assert_eq!(columns, vec![vec![Some(34), Some(-150), Some(9999)]]);
    }
}

#[test]
fn test_decimal_layout_legacy_reads() {
    assert_reads_legacy_values("legacy");
}

#[test]
fn test_decimal_layout_bloom_filter_streams_are_allowed() {
    assert_reads_legacy_values("bloom_filter");
}

#[test]
fn test_decimal_layout_precision_19_in_decimal64_file_keeps_secondary_stream() {
    assert_reads_legacy_values("legacy_p19_decimal64_version");
}

#[test]
fn test_decimal_layout_refuses_secondary_kind_changed_to_data() {
    assert_refused(
        "legacy_secondary_as_data",
        "more than one Some(Data) stream",
    );
}

#[test]
fn test_decimal_layout_refuses_extra_secondary_stream() {
    assert_refused(
        "legacy_extra_secondary",
        "more than one Some(Secondary) stream",
    );
}

#[test]
fn test_decimal_layout_refuses_extra_data_stream() {
    assert_refused("legacy_extra_data", "more than one Some(Data) stream");
}

#[test]
fn test_decimal_layout_refuses_triple_data_stream() {
    assert_refused("legacy_triple_data", "more than one Some(Data) stream");
}

#[test]
fn test_decimal_layout_refuses_unexpected_stream_kind() {
    assert_refused("legacy_extra_length", "unexpected Some(Length) stream");
}

#[test]
fn test_decimal_layout_refuses_data_kind_changed_to_secondary() {
    assert_refused(
        "legacy_data_as_secondary",
        "more than one Some(Secondary) stream",
    );
}

#[test]
fn test_decimal_layout_refuses_secondary_stream_missing() {
    assert_refused(
        "legacy_secondary_as_bloom_filter",
        "needs both a DATA and a SECONDARY",
    );
}

#[test]
fn test_decimal_layout_refuses_data_stream_missing() {
    assert_refused(
        "legacy_data_as_bloom_filter",
        "needs both a DATA and a SECONDARY",
    );
}

#[test]
fn test_decimal_layout_refuses_secondary_kind_changed_to_length() {
    assert_refused("legacy_secondary_missing", "unexpected Some(Length) stream");
}

#[test]
fn test_decimal_layout_refuses_decimal64_file_version_with_secondary() {
    assert_refused(
        "legacy_decimal64_version",
        "SECONDARY stream in a Decimal64 file",
    );
}

#[test]
fn test_decimal_layout_refuses_precision_18_decimal64_file_version_with_secondary() {
    assert_refused(
        "legacy_p18_decimal64_version",
        "SECONDARY stream in a Decimal64 file",
    );
}

#[test]
fn test_decimal_layout_refuses_scale_13_above_precision() {
    assert_refused("scale_13", "invalid decimal type (precision 12, scale 13)");
}

#[test]
fn test_decimal_layout_refuses_scale_127_above_precision() {
    assert_refused(
        "scale_127",
        "invalid decimal type (precision 12, scale 127)",
    );
}

#[test]
fn test_decimal_layout_refuses_scale_128_wraps_i8() {
    assert_refused(
        "scale_128",
        "invalid decimal type (precision 12, scale 128)",
    );
}

#[test]
fn test_decimal_layout_refuses_scale_255_wraps_i8() {
    assert_refused(
        "scale_255",
        "invalid decimal type (precision 12, scale 255)",
    );
}

#[test]
fn test_decimal_layout_refuses_scale_256_wraps_u8() {
    assert_refused(
        "scale_256",
        "invalid decimal type (precision 12, scale 256)",
    );
}

#[test]
fn test_decimal_layout_refuses_scale_above_precision() {
    assert_refused(
        "scale_above_precision",
        "invalid decimal type (precision 12, scale 13)",
    );
}

#[test]
fn test_decimal_layout_refuses_precision_39() {
    assert_refused(
        "precision_39",
        "invalid decimal type (precision 39, scale 2)",
    );
}

#[test]
fn test_decimal_layout_refuses_precision_256_wraps_u8() {
    assert_refused(
        "precision_256",
        "invalid decimal type (precision 256, scale 2)",
    );
}

#[test]
fn test_decimal_layout_refuses_unspecified_precision_scale_128() {
    assert_refused(
        "precision_unspecified_scale_128",
        "invalid decimal type (precision 38, scale 128)",
    );
}

#[test]
fn test_decimal_layout_refuses_decimal64_extra_secondary_stream() {
    assert_refused(
        "d64_extra_secondary",
        "SECONDARY stream in a Decimal64 file",
    );
}

#[test]
fn test_decimal_layout_refuses_decimal64_duplicate_data_stream() {
    assert_refused("d64_extra_data", "more than one Some(Data) stream");
}

#[test]
fn test_decimal_layout_refuses_decimal64_unexpected_stream_kind() {
    assert_refused("d64_extra_length", "unexpected Some(Length) stream");
}

#[test]
fn test_decimal_layout_refuses_decimal64_data_kind_changed_to_secondary() {
    assert_refused(
        "d64_data_as_secondary",
        "SECONDARY stream in a Decimal64 file",
    );
}

#[test]
fn test_decimal_layout_refuses_decimal64_data_kind_changed_to_length() {
    assert_refused("d64_data_as_length", "unexpected Some(Length) stream");
}

#[test]
fn test_decimal_layout_refuses_decimal64_dictionary_encoding() {
    assert_refused("d64_dictionary_encoding", "has encoding DictionaryV2");
}

#[test]
fn test_decimal_layout_refuses_decimal64_streams_in_legacy_version_file() {
    assert_refused("d64_legacy_version", "needs both a DATA and a SECONDARY");
}

#[test]
fn test_decimal_layout_refuses_decimal64_file_wide_column_duplicate_data_stream() {
    assert_refused("d64_wide_extra_data", "more than one Some(Data) stream");
}

#[test]
fn test_decimal_layout_refuses_decimal64_scale_128_wraps_i8() {
    assert_refused(
        "d64_scale_128",
        "invalid decimal type (precision 12, scale 128)",
    );
}

#[test]
fn test_decimal_layout_refuses_decimal64_scale_256_wraps_u8() {
    assert_refused(
        "d64_scale_256",
        "invalid decimal type (precision 12, scale 256)",
    );
}

#[test]
fn test_decimal_layout_refuses_decimal64_scale_13_above_precision() {
    assert_refused(
        "d64_scale_13",
        "invalid decimal type (precision 12, scale 13)",
    );
}

#[cfg(feature = "async")]
async fn read_all_async(path: &str) -> Result<usize, Box<dyn std::error::Error>> {
    use futures_util::TryStreamExt;

    let f = tokio::fs::File::open(basic_path(path)).await?;
    let reader = ArrowReaderBuilder::try_new_async(f).await?.build_async();
    let batches = reader.try_collect::<Vec<_>>().await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_decimal_layout_async_reads_decimal64_file() {
    assert_eq!(read_all_async("decimal64_v2.orc").await.unwrap(), 10);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_decimal_layout_async_refuses_damaged_decimal64_file() {
    let error = read_all_async("decimal_layout_d64_extra_secondary.orc")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("SECONDARY stream in a Decimal64 file"),
        "{error}"
    );
}
