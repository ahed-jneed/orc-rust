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

use arrow::array::{Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use orc_rust::arrow_reader::ArrowReaderBuilder;

use crate::basic_path;

fn read_columns(path: &Path, batch_size: usize) -> Result<(Vec<Option<i64>>, Vec<i64>), String> {
    let reader = ArrowReaderBuilder::try_new(File::open(path).unwrap())
        .map_err(|e| e.to_string())?
        .with_batch_size(batch_size)
        .build();
    let mut values = vec![];
    let mut rows = vec![];
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        let v = cast(batch.column_by_name("v").unwrap(), &DataType::Int64).unwrap();
        let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend((0..v.len()).map(|i| v.is_valid(i).then(|| v.value(i))));
        let x = batch.column_by_name("x").unwrap();
        let x = x.as_any().downcast_ref::<Int64Array>().unwrap();
        rows.extend(x.values().iter().copied());
    }
    Ok((values, rows))
}

fn check_property_dir(dir: &Path, batch_sizes: &[usize]) -> usize {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "orc"))
        .collect();
    files.sort();
    for path in &files {
        let expected = fs::read_to_string(path.with_extension("expected")).unwrap();
        let expected: Vec<Option<i64>> = expected
            .lines()
            .skip(1)
            .map(|line| (line != "null").then(|| line.parse().unwrap()))
            .collect();
        for &batch_size in batch_sizes {
            let (actual, rows) = read_columns(path, batch_size)
                .unwrap_or_else(|e| panic!("{path:?} batch {batch_size}: {e}"));
            assert_eq!(actual.len(), expected.len(), "{path:?} batch {batch_size}");
            if let Some(row) = (0..actual.len()).find(|&i| actual[i] != expected[i]) {
                panic!(
                    "{path:?} batch {batch_size} row {row}: {:?} != {:?}",
                    actual[row], expected[row]
                );
            }
            assert!(rows.iter().copied().eq(0..rows.len() as i64), "{path:?}");
        }
    }
    files.len()
}

#[test]
fn rle_v1_wrapping_minimal_fixture() {
    let path = basic_path("rle_v1_wrapping/minimal_bigint_v011.orc");
    for batch_size in [1, 3, 8192] {
        let (values, rows) = read_columns(Path::new(&path), batch_size).unwrap();
        assert_eq!(
            values,
            vec![
                Some(-9223372036854775808),
                Some(9223372036854775807),
                Some(9223372036854775806)
            ]
        );
        assert_eq!(rows, vec![0, 1, 2]);
    }
}

#[test]
fn rle_v1_wrapping_property_fixtures() {
    let dir = Path::new(&basic_path("rle_v1_wrapping")).to_path_buf();
    assert!(check_property_dir(&dir, &[1, 3, 8192]) >= 9);
}

#[test]
#[ignore = "requires ORC_RLE_V1_PROPERTY_DIR from scripts/generate_orc_rle_v1_property.py"]
fn rle_v1_wrapping_property_generated() {
    let dir = std::env::var("ORC_RLE_V1_PROPERTY_DIR").unwrap();
    let files = check_property_dir(Path::new(&dir), &[8192, 777]);
    assert!(files >= 300, "only {files} files");
}
