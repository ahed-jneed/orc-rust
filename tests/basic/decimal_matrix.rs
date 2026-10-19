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
use arrow::compute::concat;
use orc_rust::arrow_reader::ArrowReaderBuilder;

const ROWS: usize = 20;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

fn expected(row: usize, precision: usize) -> Option<i128> {
    if row % 5 == 4 {
        return None;
    }
    let mut v: i128 = 0;
    for k in 0..precision {
        let digit = if row == ROWS - 1 {
            9
        } else {
            ((row + 1) * 7 + k * 3) % 10
        };
        v = v * 10 + digit as i128;
    }
    Some(if row % 3 == 1 { -v } else { v })
}

#[test]
fn test_pyorc_decimal_matrix_is_unchanged() {
    for batch_size in [1, 7, 8192] {
        let f = File::open(basic_path("pyorc_decimal_matrix.orc")).unwrap();
        let reader = ArrowReaderBuilder::try_new(f)
            .unwrap()
            .with_batch_size(batch_size)
            .build();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 779);
        for precision in 1..=38usize {
            for scale in 0..=precision {
                let name = format!("p{precision}_s{scale}");
                let index = schema.index_of(&name).unwrap();
                let parts: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(index).as_ref()).collect();
                let joined = concat(&parts).unwrap();
                let joined = joined.as_any().downcast_ref::<Decimal128Array>().unwrap();
                assert_eq!(joined.precision() as usize, precision, "{name}");
                assert_eq!(joined.scale() as usize, scale, "{name}");
                assert_eq!(joined.len(), ROWS);
                for row in 0..ROWS {
                    let want = expected(row, precision);
                    let got = (!joined.is_null(row)).then(|| joined.value(row));
                    assert_eq!(got, want, "{name} row {row} batch {batch_size}");
                }
            }
        }
    }
}

#[test]
fn test_invalid_decimal_type_names_the_column() {
    for file in [
        "pyorc_decimal_invalid_p39.orc",
        "pyorc_decimal_invalid_scale_gt_precision.orc",
    ] {
        let f = File::open(basic_path(file)).unwrap();
        let message = match ArrowReaderBuilder::try_new(f) {
            Err(e) => e.to_string(),
            Ok(builder) => {
                let mut reader = builder.build();
                reader.find_map(|b| b.err()).unwrap().to_string()
            }
        };
        assert!(message.contains("Column 'd'"), "{file}: {message}");
        assert!(!message.contains("Arrow error"), "{file}: {message}");
    }
}
