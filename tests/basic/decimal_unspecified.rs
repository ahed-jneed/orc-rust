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

use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::compute::concat;
use arrow::datatypes::DataType as ArrowDataType;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::schema::DataType;

const SPLIT_ELIM_USERID: [i64; 100] = [
    2, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    100, 100, 100, 100, 100,
];
const SPLIT_ELIM_STRING1: [&str; 100] = [
    "foo", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
    "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra", "zebra",
];
const SPLIT_ELIM_SUBTYPE: [f64; 100] = [
    0.8, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0,
    8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0,
    8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0,
    8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0,
    8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0,
    8.0, 8.0, 8.0, 8.0, 8.0,
];
const PYARROW_SPLIT_ELIM_DECIMAL1: [i128; 100] = [
    1200000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0,
];
const PYARROW_SPLIT_ELIM_DECIMAL1_SUM: i128 = 16600000;
const PYARROW_FORMAT_11_DECIMAL1: [i128; 100] = [
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
    12345678654745,
];
const FORMAT_11_LONG1: [i64; 100] = [
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
    9223372036854775807,
];
const PYARROW_FORMAT_11_DECIMAL1_SUM: i128 = 92592589910587500;

const DEFAULT_SCALE: u32 = 10;
const PYARROW_SCALE: u32 = 6;
const SPLIT_ELIM_NONZERO: [(usize, i128); 5] = [
    (0, 12_000_000_000),
    (5000, 22_000_000_000),
    (10000, 33_000_000_000),
    (15000, 44_000_000_000),
    (20000, 55_000_000_000),
];
const FORMAT_11_EVEN: i128 = 123_456_786_547_456_0;
const FORMAT_11_ODD: i128 = 123_456_786_547_457_0;

const SPLIT_ELIM_ROWS: usize = 25_000;
const FORMAT_11_ROWS: usize = 7_500;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

fn read_all(file: &str, column: &str, batch_size: usize) -> Box<dyn Array> {
    let f = File::open(basic_path(file)).unwrap();
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_batch_size(batch_size)
        .build();
    let mut parts = vec![];
    for batch in reader {
        let batch = batch.unwrap();
        let index = batch.schema().index_of(column).unwrap();
        parts.push(batch.column(index).clone());
    }
    let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
    Box::new(arrow::array::make_array(concat(&refs).unwrap().to_data()))
}

fn decimals(file: &str, batch_size: usize) -> Decimal128Array {
    let array = read_all(file, "decimal1", batch_size);
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .clone()
}

fn assert_pyarrow_cells(d: &Decimal128Array, first_100: &[i128; 100], sum: i128) {
    let factor = 10_i128.pow(DEFAULT_SCALE - PYARROW_SCALE);
    for (i, expected) in first_100.iter().enumerate() {
        assert_eq!(d.value(i) / factor, *expected, "row {i}");
    }
    let total: i128 = d.values().iter().map(|v| v / factor).sum();
    assert_eq!(total, sum);
}

fn check_schema(file: &str) {
    let f = File::open(basic_path(file)).unwrap();
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let schema = builder.schema();
    let field = schema.field_with_name("decimal1").unwrap();
    assert_eq!(field.data_type(), &ArrowDataType::Decimal128(38, 10));
    let children = builder.file_metadata().root_data_type().children();
    let child = children.iter().find(|c| c.name() == "decimal1").unwrap();
    assert!(matches!(
        child.data_type(),
        DataType::Decimal {
            precision: 38,
            scale: 10,
            ..
        }
    ));
}

#[test]
fn test_unspecified_decimal_schema() {
    check_schema("orc_split_elim.orc");
    check_schema("orc-file-11-format.orc");
}

#[test]
fn test_unspecified_decimal_split_elim() {
    for batch_size in [1, 3, 8192] {
        let file = "orc_split_elim.orc";
        let d = decimals(file, batch_size);
        assert_eq!(d.len(), SPLIT_ELIM_ROWS, "batch {batch_size}");
        assert_eq!((d.precision(), d.scale()), (38, 10));
        assert_eq!(d.null_count(), 0);
        for (i, v) in d.values().iter().enumerate() {
            let expected = SPLIT_ELIM_NONZERO
                .iter()
                .find(|(row, _)| *row == i)
                .map_or(0, |(_, v)| *v);
            assert_eq!(*v, expected, "row {i} batch {batch_size}");
        }
        assert_pyarrow_cells(
            &d,
            &PYARROW_SPLIT_ELIM_DECIMAL1,
            PYARROW_SPLIT_ELIM_DECIMAL1_SUM,
        );

        let a = read_all(file, "userid", batch_size);
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&a.values()[..100], &SPLIT_ELIM_USERID);
        let a = read_all(file, "string1", batch_size);
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        for (i, expected) in SPLIT_ELIM_STRING1.iter().enumerate() {
            assert_eq!(a.value(i), *expected);
        }
        let a = read_all(file, "subtype", batch_size);
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(&a.values()[..100], &SPLIT_ELIM_SUBTYPE);
    }
}

#[test]
fn test_unspecified_decimal_format_11() {
    for batch_size in [1, 3, 8192] {
        let file = "orc-file-11-format.orc";
        let d = decimals(file, batch_size);
        assert_eq!(d.len(), FORMAT_11_ROWS, "batch {batch_size}");
        assert_eq!((d.precision(), d.scale()), (38, 10));
        assert_eq!(d.null_count(), 0);
        for (i, v) in d.values().iter().enumerate() {
            let expected = if i % 2 == 0 {
                FORMAT_11_EVEN
            } else {
                FORMAT_11_ODD
            };
            assert_eq!(*v, expected, "row {i} batch {batch_size}");
        }
        assert_pyarrow_cells(
            &d,
            &PYARROW_FORMAT_11_DECIMAL1,
            PYARROW_FORMAT_11_DECIMAL1_SUM,
        );

        let a = read_all(file, "long1", batch_size);
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&a.values()[..100], &FORMAT_11_LONG1);
    }
}
