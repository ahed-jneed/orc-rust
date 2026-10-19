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

use arrow::array::{Array, Decimal128Array, Int64Array};
use arrow::compute::concat;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

struct Expected {
    name: &'static str,
    precision: u8,
    scale: i8,
    values: &'static [i128],
}

fn read_column(file: &str, column: &str, batch_size: usize) -> Vec<Box<dyn Array>> {
    let f = File::open(basic_path(file)).unwrap();
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let projection =
        ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &[column]);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build();
    reader
        .map(|b| b.unwrap().column(0).to_data())
        .map(|d| Box::new(arrow::array::make_array(d)) as Box<dyn Array>)
        .collect()
}

fn check(file: &str, ints: &[i64], decimals: &[Expected]) {
    for batch_size in [1, 3, 8192] {
        let parts = read_column(file, "a", batch_size);
        let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
        let joined = concat(&refs).unwrap();
        let joined = joined.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            joined.values().as_ref(),
            ints,
            "{file} a batch {batch_size}"
        );
        for e in decimals {
            let parts = read_column(file, e.name, batch_size);
            let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
            let joined = concat(&refs).unwrap();
            let joined = joined.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(
                joined.precision(),
                e.precision,
                "{file} {} precision",
                e.name
            );
            assert_eq!(joined.scale(), e.scale, "{file} {} scale", e.name);
            assert_eq!(joined.null_count(), 0);
            assert_eq!(
                joined.values().as_ref(),
                e.values,
                "{file} {} batch {batch_size}",
                e.name
            );
        }
    }
}

const DECIMAL64_V2_A: [i64; 10] = [
    17292380420,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380422,
    17292380422,
    17292380422,
];

const DECIMAL64_V2_B: [i128; 10] = [24, 38, 28, 40, 2, 42, 10, 12, 41, 38];

const DECIMAL64_V2_C: [i128; 10] = [
    3616416, 6335170, 4267396, 7667760, 209648, 4528482, 1857290, 1483680, 8215252, 4724084,
];

const DECIMAL64_V2_D: [i128; 10] = [3, 8, 9, 5, 7, 7, 1, 9, 7, 10];

const DECIMAL64_V2_E: [i128; 10] = [1, 1, 6, 4, 7, 5, 8, 6, 2, 0];

#[test]
fn test_decimal64_v2_all_values() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[
            Expected {
                name: "b",
                precision: 12,
                scale: 0,
                values: &DECIMAL64_V2_B,
            },
            Expected {
                name: "c",
                precision: 20,
                scale: 2,
                values: &DECIMAL64_V2_C,
            },
            Expected {
                name: "d",
                precision: 12,
                scale: 2,
                values: &DECIMAL64_V2_D,
            },
            Expected {
                name: "e",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_E,
            },
        ],
    );
}

#[test]
fn test_decimal64_v2_column_b() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "b",
            precision: 12,
            scale: 0,
            values: &DECIMAL64_V2_B,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_c() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "c",
            precision: 20,
            scale: 2,
            values: &DECIMAL64_V2_C,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_d() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "d",
            precision: 12,
            scale: 2,
            values: &DECIMAL64_V2_D,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_e() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "e",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_E,
        }],
    );
}

const DECIMAL64_V2_CPLUSPLUS_A: [i64; 10] = [
    17292380420,
    17292380421,
    17292380422,
    17292380423,
    17292380424,
    17292380425,
    17292380426,
    17292380427,
    17292380428,
    17292380429,
];

const DECIMAL64_V2_CPLUSPLUS_B: [i128; 10] = [
    16416, 32832, 65664, 131328, 262656, 525312, 1050624, 2101248, 4202496, 8404992,
];

const DECIMAL64_V2_CPLUSPLUS_C: [i128; 10] = [3, 8, 9, 5, 7, 7, 1, 9, 7, 12];

const DECIMAL64_V2_CPLUSPLUS_D: [i128; 10] = [1, 1, 6, 4, 7, 5, 8, 6, 2, 34];

const DECIMAL64_V2_CPLUSPLUS_E: [i128; 10] = [4, 9, 15, 9, 14, 12, 9, 15, 9, 46];

#[test]
fn test_decimal64_v2_cplusplus_all_values() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[
            Expected {
                name: "b",
                precision: 10,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_B,
            },
            Expected {
                name: "c",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_C,
            },
            Expected {
                name: "d",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_D,
            },
            Expected {
                name: "e",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_E,
            },
        ],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_b() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "b",
            precision: 10,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_B,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_c() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "c",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_C,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_d() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "d",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_D,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_e() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "e",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_E,
        }],
    );
}
