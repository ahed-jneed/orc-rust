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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

use crate::basic_path;

const BATCH_SIZES: [usize; 3] = [1, 3, 8192];

fn render(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "~".to_string();
    }
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(row).to_string()
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            (if a.value(row) { "T" } else { "F" }).to_string()
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(row).to_string()
        }
        DataType::Struct(fields) => {
            let a = array.as_any().downcast_ref::<StructArray>().unwrap();
            let cells = fields
                .iter()
                .zip(a.columns())
                .map(|(field, column)| format!("{}={}", field.name(), render(column, row)))
                .collect::<Vec<_>>();
            format!("{{{}}}", cells.join(","))
        }
        DataType::List(_) => {
            let a = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = a.value(row);
            let cells = (0..values.len())
                .map(|i| render(&values, i))
                .collect::<Vec<_>>();
            format!("[{}]", cells.join(","))
        }
        other => panic!("unexpected type {other}"),
    }
}

fn read(path: &str, batch_size: usize, projection: Option<&[&str]>) -> Vec<RecordBatch> {
    let builder = ArrowReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .with_batch_size(batch_size);
    let builder = match projection {
        Some(names) => {
            let mask = ProjectionMask::named_roots(builder.file_metadata().root_data_type(), names);
            builder.with_projection(mask)
        }
        None => builder,
    };
    let batches = builder
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("{path} batch_size {batch_size}: {e}"));
    for batch in &batches {
        assert!(batch.num_rows() <= batch_size);
    }
    batches
}

fn rendered(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        assert_eq!(column.len(), batch.num_rows());
        out.extend((0..column.len()).map(|i| render(column, i)));
    }
    out
}

fn expected(path: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let text = fs::read_to_string(path.replace(".orc", ".expected")).unwrap();
    let mut lines = text.lines();
    let names = lines
        .next()
        .unwrap()
        .split('\t')
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut columns = vec![Vec::new(); names.len()];
    for line in lines {
        let cells = line.split('\t').collect::<Vec<_>>();
        assert_eq!(cells.len(), names.len());
        for (column, cell) in columns.iter_mut().zip(cells) {
            column.push(cell.to_string());
        }
    }
    (names, columns)
}

fn check_file(path: &str, batch_sizes: &[usize]) {
    let (names, columns) = expected(path);
    let rows = columns[0].len();
    let mut projections: Vec<Vec<&str>> = vec![names.iter().map(String::as_str).collect()];
    projections.extend(names.iter().map(|n| vec![n.as_str()]));
    if names.len() > 2 {
        projections.push(vec![names[0].as_str(), names[names.len() - 1].as_str()]);
    }
    for &batch_size in batch_sizes {
        for projection in &projections {
            let batches = read(path, batch_size, Some(projection));
            let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, rows, "{path} batch_size {batch_size} {projection:?}");
            for name in projection {
                let index = names.iter().position(|n| n == name).unwrap();
                assert_eq!(
                    rendered(&batches, name),
                    columns[index],
                    "{path} batch_size {batch_size} {projection:?} column {name}"
                );
            }
        }
        let batches = read(path, batch_size, None);
        for (name, column) in names.iter().zip(&columns) {
            assert_eq!(&rendered(&batches, name), column, "{path} {name}");
        }
    }
}

fn data_path(name: &str) -> String {
    basic_path(&format!("empty_struct/{name}.orc"))
}

fn stripe_count(path: &str) -> usize {
    ArrowReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .file_metadata()
        .stripe_metadatas()
        .len()
}

#[test]
fn empty_struct_top_level() {
    let path = data_path("es");
    for batch_size in BATCH_SIZES {
        let batches = read(&path, batch_size, None);
        let mut ids = Vec::new();
        let mut nulls = Vec::new();
        for batch in &batches {
            let a = batch.column_by_name("a").unwrap().as_struct();
            assert_eq!(a.len(), batch.num_rows());
            assert_eq!(a.num_columns(), 0);
            nulls.extend((0..a.len()).map(|i| a.is_null(i)));
            let id = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive_opt::<arrow::datatypes::Int64Type>();
            ids.extend(id.unwrap().values().iter().copied());
        }
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(nulls, vec![false, true, false]);
    }
}

#[test]
fn empty_struct_only_projection() {
    let path = data_path("es");
    for batch_size in BATCH_SIZES {
        let batches = read(&path, batch_size, Some(&["a"]));
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(rendered(&batches, "a"), ["{}", "~", "{}"]);
        let batches = read(&path, batch_size, Some(&["id"]));
        assert_eq!(rendered(&batches, "id"), ["1", "2", "3"]);
    }
}

#[test]
fn empty_struct_in_list() {
    check_file(&data_path("es_list"), &BATCH_SIZES);
    let batches = read(&data_path("es_list"), 8192, Some(&["l"]));
    let l = batches[0].column_by_name("l").unwrap().as_list::<i32>();
    assert_eq!(l.len(), 5);
    assert_eq!(l.null_count(), 1);
    assert!(l.is_null(1));
    let lengths = (0..5).map(|i| l.value(i).len()).collect::<Vec<_>>();
    assert_eq!(lengths, [2, 0, 0, 3, 1]);
    let element: ArrayRef = l.values().clone();
    assert_eq!(element.len(), 6);
    assert_eq!(element.null_count(), 2);
}

#[test]
fn empty_struct_in_struct() {
    check_file(&data_path("es_nested"), &BATCH_SIZES);
}

#[test]
fn empty_struct_multi_stripe() {
    let path = data_path("es_multistripe");
    assert!(stripe_count(&path) > 1);
    check_file(&path, &BATCH_SIZES);
}

#[test]
fn empty_struct_fixtures_match_pyarrow() {
    let dir = basic_path("empty_struct");
    let mut checked = 0;
    for entry in fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "orc") {
            check_file(path.to_str().unwrap(), &BATCH_SIZES);
            checked += 1;
        }
    }
    assert!(checked >= 10);
}

#[test]
#[ignore = "requires ORC_EMPTY_STRUCT_PROPERTY_DIR from scripts/generate_orc_empty_struct.py"]
fn empty_struct_property_generated() {
    let dir = std::env::var("ORC_EMPTY_STRUCT_PROPERTY_DIR").unwrap();
    let mut checked = 0;
    for entry in fs::read_dir(Path::new(&dir)).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "orc") {
            check_file(path.to_str().unwrap(), &[8192, 777]);
            checked += 1;
        }
    }
    assert!(checked >= 100, "only {checked} files");
}
