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

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Decimal128Type, Field, Schema, TimeUnit};
use chrono::NaiveDateTime;
use orc_rust::arrow_reader::ArrowReaderBuilder;

use crate::basic_path;

fn nanos(value: &str) -> i128 {
    let value = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap()
        .and_utc();
    i128::from(value.timestamp()) * 1_000_000_000 + i128::from(value.timestamp_subsec_nanos())
}

fn read(path: &Path, batch_size: usize, data_type: &DataType) -> Vec<Option<i128>> {
    let schema = Arc::new(Schema::new(vec![Field::new("t", data_type.clone(), true)]));
    ArrowReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .with_schema(schema)
        .with_batch_size(batch_size)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            let array = match data_type {
                DataType::Decimal128(..) => batch.column(0).clone(),
                _ => cast(batch.column(0), &DataType::Decimal128(38, 0)).unwrap(),
            };
            let array = array.as_primitive::<Decimal128Type>();
            (0..array.len())
                .map(|i| array.is_valid(i).then(|| array.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn check_dir(dir: &str) -> usize {
    let dir = basic_path(dir);
    let mut files: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "orc"))
        .collect();
    files.sort();
    let types = [
        (DataType::Timestamp(TimeUnit::Nanosecond, None), 1),
        (DataType::Timestamp(TimeUnit::Microsecond, None), 1_000),
        (DataType::Timestamp(TimeUnit::Millisecond, None), 1_000_000),
        (DataType::Timestamp(TimeUnit::Second, None), 1_000_000_000),
        (DataType::Decimal128(38, 9), -1),
    ];
    for path in &files {
        let expected = fs::read_to_string(path.with_extension("expected")).unwrap();
        let expected: Vec<_> = expected.lines().skip(1).map(nanos).collect();
        for (data_type, divisor) in &types {
            let wanted: Vec<_> = expected
                .iter()
                .map(|&v| {
                    Some(if *divisor < 0 {
                        v
                    } else {
                        v.div_euclid(*divisor)
                    })
                })
                .collect();
            for batch_size in [1, 7, 8192] {
                let actual = read(path, batch_size, data_type);
                assert_eq!(
                    actual.len(),
                    wanted.len(),
                    "{path:?} {data_type} batch {batch_size}"
                );
                let compared =
                    |i: usize| *divisor != 1_000_000_000 || expected[i] % 1_000_000_000 == 0;
                if let Some(row) =
                    (0..actual.len()).find(|&i| compared(i) && actual[i] != wanted[i])
                {
                    panic!(
                        "{path:?} {data_type} batch {batch_size} row {row}: {:?} != {:?}",
                        actual[row], wanted[row]
                    );
                }
            }
        }
    }
    files.len()
}

#[test]
fn timestamp_dst_zones_after_2099() {
    assert_eq!(check_dir("timestamp_zones"), 14);
}

#[test]
fn timestamp_java_writer_zones_before_first_transition() {
    assert_eq!(check_dir("timestamp_zones_java"), 6);
}
