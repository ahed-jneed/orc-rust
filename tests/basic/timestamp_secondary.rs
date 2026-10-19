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

use arrow::array::{Array, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

use crate::basic_path;

const MIXED_EXPECTED: [&str; 20] = [
    "1954-06-11T04:27:22.896626",
    "2000-12-24T04:31:11",
    "2005-10-28T07:33:29",
    "2073-06-19T12:32:27.332276",
    "2022-11-04T04:21:12.270611",
    "1996-10-30T01:43:19.403296",
    "2060-12-14T09:49:03",
    "1914-08-19T00:09:07",
    "2058-09-30T05:59:46.745912",
    "2041-04-18T00:24:52.669254",
    "1931-03-15T18:32:37",
    "1989-04-19T04:01:16.440696",
    "2098-06-13T23:21:20",
    "2109-12-22T11:28:24",
    "2072-02-10T11:35:25.341315",
    "1986-10-17T19:26:40",
    "1947-12-09T22:41:38",
    "2019-10-14T13:49:02.011674",
    "1983-01-14T18:00:18.958435",
    "1928-06-25T02:18:33",
];

fn nanos(value: &str) -> i64 {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap()
}

fn read(path: &str, batch_size: usize, projected: bool) -> Vec<RecordBatch> {
    let builder = ArrowReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let builder = if projected {
        let mask = ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &["t"]);
        builder.with_projection(mask)
    } else {
        builder
    };
    builder
        .with_batch_size(batch_size)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn values(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            (0..array.len())
                .map(|i| array.is_valid(i).then(|| array.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn timestamp_mixed_whole_and_fractional_seconds() {
    let path = basic_path("timestamp_secondary_mixed.orc");
    let expected: Vec<_> = MIXED_EXPECTED.iter().map(|v| Some(nanos(v))).collect();
    for projected in [false, true] {
        for batch_size in [1, 3, 8192] {
            let actual = values(&read(&path, batch_size, projected));
            assert_eq!(
                actual, expected,
                "projected={projected} batch_size={batch_size}"
            );
        }
    }
}
