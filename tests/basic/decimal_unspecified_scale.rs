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
use arrow::datatypes::DataType as ArrowDataType;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

const LOSSY: &str = "pyorc_decimal_unspecified_lossy.orc";
const EXACT: &str = "pyorc_decimal_unspecified_exact.orc";
const OVERFLOW: &str = "pyorc_decimal_unspecified_overflow.orc";
const DEFAULT_SCALE: u32 = 10;
const SCALES: [u32; 9] = [0, 3, 6, 7, 10, 11, 18, 19, 38];
const BATCH_SIZES: [usize; 3] = [1, 3, 8192];

fn read_column(file: &str, column: &str, batch_size: usize) -> Result<Decimal128Array, String> {
    let path = format!("{}/tests/basic/data/{file}", env!("CARGO_MANIFEST_DIR"));
    let builder = ArrowReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let projection =
        ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &[column]);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build();
    let mut values = vec![];
    let mut data_type = None;
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        let array = batch.column(0);
        data_type = Some(array.data_type().clone());
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        values.extend_from_slice(array.values());
    }
    let data_type = data_type.unwrap_or(ArrowDataType::Decimal128(38, DEFAULT_SCALE as i8));
    let ArrowDataType::Decimal128(precision, scale) = data_type else {
        panic!("not a decimal");
    };
    Ok(Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .unwrap())
}

fn pow10(exponent: u32) -> i128 {
    10_i128.pow(exponent)
}

fn lossy_unscaled(scale: u32) -> Vec<i128> {
    let integer = if scale == 38 { 0 } else { pow10(scale) };
    vec![0, 1, -1, integer + 5, 12345, -98765]
}

fn exact_unscaled(scale: u32) -> Vec<i128> {
    match scale {
        0 => vec![0, 1, -3, 2],
        38 => vec![0, 5 * pow10(37), -5 * pow10(37), 25 * pow10(36)],
        s => vec![
            0,
            12 * pow10(s - 1),
            -35 * pow10(s - 1),
            if s == 1 { 3 } else { 25 * pow10(s - 2) },
        ],
    }
}

fn to_default_scale(values: &[i128], scale: u32) -> Vec<i128> {
    values
        .iter()
        .map(|v| {
            if scale <= DEFAULT_SCALE {
                v * pow10(DEFAULT_SCALE - scale)
            } else {
                assert_eq!(v % pow10(scale - DEFAULT_SCALE), 0);
                v / pow10(scale - DEFAULT_SCALE)
            }
        })
        .collect()
}

fn error_of(file: &str, column: &str, batch_size: usize) -> String {
    match read_column(file, column, batch_size) {
        Ok(_) => panic!("{column} batch {batch_size} was read"),
        Err(e) => e,
    }
}

#[test]
fn test_unspecified_decimal_scales_up_to_the_default_are_exact() {
    for scale in SCALES.into_iter().filter(|s| *s <= DEFAULT_SCALE) {
        for batch_size in BATCH_SIZES {
            let name = format!("s{scale}");
            let d = read_column(LOSSY, &name, batch_size).unwrap();
            assert_eq!((d.precision(), d.scale()), (38, DEFAULT_SCALE as i8));
            assert_eq!(
                d.values().to_vec(),
                to_default_scale(&lossy_unscaled(scale), scale),
                "{name} batch {batch_size}"
            );
        }
    }
}

#[test]
fn test_unspecified_decimal_scales_up_to_six_equal_pyarrow() {
    for scale in SCALES.into_iter().filter(|s| *s <= 6) {
        let d = read_column(LOSSY, &format!("s{scale}"), 8192).unwrap();
        let in_pyarrow_scale: Vec<i128> = d.values().iter().map(|v| v / pow10(4)).collect();
        assert_eq!(
            in_pyarrow_scale,
            to_pyarrow_scale(&lossy_unscaled(scale), scale),
            "s{scale}"
        );
    }
}

fn to_pyarrow_scale(values: &[i128], scale: u32) -> Vec<i128> {
    values
        .iter()
        .map(|v| {
            if scale <= 6 {
                v * pow10(6 - scale)
            } else {
                v / pow10(scale - 6)
            }
        })
        .collect()
}

#[test]
fn test_unspecified_decimal_scales_above_the_default_with_remainder_are_refused() {
    for scale in SCALES.into_iter().filter(|s| *s > DEFAULT_SCALE) {
        for batch_size in BATCH_SIZES {
            let name = format!("s{scale}");
            let message = error_of(LOSSY, &name, batch_size);
            assert!(message.contains(&format!("'{name}'")), "{message}");
            assert!(message.contains(&format!("scale {scale}")), "{message}");
            assert!(
                message.contains(&format!("scale {DEFAULT_SCALE}")),
                "{message}"
            );
        }
    }
}

#[test]
fn test_unspecified_decimal_scale_boundary_at_the_default() {
    let d = read_column(LOSSY, "s10", 8192).unwrap();
    assert_eq!(d.value(1), 1);
    assert_eq!(d.value(3), 10_000_000_005);
    let message = error_of(LOSSY, "s11", 8192);
    assert!(message.contains("scale 11"), "{message}");
    assert!(message.contains("scale 10"), "{message}");
    let d = read_column(LOSSY, "s7", 8192).unwrap();
    assert_eq!(d.value(1), 1000);
    assert_eq!(d.value(3), 10_000_005_000);
    let d = read_column(LOSSY, "s6", 8192).unwrap();
    assert_eq!(d.value(1), 10_000);
}

#[test]
fn test_unspecified_decimal_scales_above_the_default_with_trailing_zeros_are_exact() {
    for scale in SCALES {
        for batch_size in BATCH_SIZES {
            let name = format!("s{scale}");
            let d = read_column(EXACT, &name, batch_size).unwrap();
            assert_eq!((d.precision(), d.scale()), (38, DEFAULT_SCALE as i8));
            assert_eq!(
                d.values().to_vec(),
                to_default_scale(&exact_unscaled(scale), scale),
                "{name} batch {batch_size}"
            );
        }
    }
}

#[test]
fn test_unspecified_decimal_overflow_is_an_error() {
    for column in ["ov38", "ov29"] {
        for batch_size in BATCH_SIZES {
            let message = error_of(OVERFLOW, column, batch_size);
            assert!(
                message.contains("overflows") || message.contains(column),
                "{message}"
            );
        }
    }
}

#[test]
fn test_recorded_scale_keeps_truncating_like_pyarrow() {
    for batch_size in BATCH_SIZES {
        let d = read_column(LOSSY, "rec12", batch_size).unwrap();
        assert_eq!((d.precision(), d.scale()), (38, 6));
        assert_eq!(
            d.values().to_vec(),
            vec![0, 1_234_567, -1, 5_500_000, 0, 0],
            "batch {batch_size}"
        );
    }
}
