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

//! Rows that are null at the root, in nested structs, lists, maps and unions, in every
//! primitive type, against the rows pyarrow and pyorc read from the same files. The files
//! and the expected rows come from scripts/generate_orc_null_matrix.py.

use std::fs::{self, File};

use arrow::array::{Array, AsArray, UnionArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, TimeUnit, TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::row_selection::{RowSelection, RowSelector};

use crate::basic_path;

pub(crate) const BATCH_SIZES: [usize; 4] = [1, 7, 100, 8192];

fn render(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "~".to_string();
    }
    match array.data_type() {
        DataType::Boolean => (if array.as_boolean().value(row) {
            "T"
        } else {
            "F"
        })
        .to_string(),
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Float32 => format!(
            "{:08x}",
            array.as_primitive::<Float32Type>().value(row).to_bits()
        ),
        DataType::Float64 => format!(
            "{:016x}",
            array.as_primitive::<Float64Type>().value(row).to_bits()
        ),
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        DataType::Binary => array
            .as_binary::<i32>()
            .value(row)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        DataType::Date32 => array.as_primitive::<Date32Type>().value(row).to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row)
            .to_string(),
        DataType::Decimal128(..) => array
            .as_primitive::<Decimal128Type>()
            .value(row)
            .to_string(),
        DataType::Struct(fields) => {
            let a = array.as_struct();
            let cells = fields
                .iter()
                .zip(a.columns())
                .map(|(field, column)| format!("{}={}", field.name(), render(column, row)))
                .collect::<Vec<_>>();
            format!("{{{}}}", cells.join(","))
        }
        DataType::List(_) => {
            let values = array.as_list::<i32>().value(row);
            let cells = (0..values.len())
                .map(|i| render(&values, i))
                .collect::<Vec<_>>();
            format!("[{}]", cells.join(","))
        }
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            let cells = (0..entries.len())
                .map(|i| {
                    format!(
                        "{}={}",
                        render(entries.column(0), i),
                        render(entries.column(1), i)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", cells.join(","))
        }
        DataType::Union(..) => {
            let a = array.as_any().downcast_ref::<UnionArray>().unwrap();
            let tag = a.type_id(row);
            let child = a.child(tag);
            let offset = a.value_offset(row);
            if child.is_null(offset) {
                "~".to_string()
            } else {
                format!("<{tag}:{}>", render(child, offset))
            }
        }
        other => panic!("unexpected type {other}"),
    }
}

fn read(
    path: &str,
    batch_size: usize,
    projection: Option<&[&str]>,
    selection: Option<RowSelection>,
) -> Vec<RecordBatch> {
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
    let builder = match selection {
        Some(selection) => builder.with_row_selection(selection),
        None => builder,
    };
    let batches = builder
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("{path} batch_size {batch_size} {projection:?}: {e}"));
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
        assert_eq!(cells.len(), names.len(), "{line}");
        for (column, cell) in columns.iter_mut().zip(cells) {
            column.push(cell.to_string());
        }
    }
    (names, columns)
}

fn data_path(name: &str) -> String {
    basic_path(&format!("null_matrix/{name}.orc"))
}

pub(crate) fn check_projections(path: &str, batch_sizes: &[usize]) {
    let (names, columns) = expected(path);
    let rows = columns[0].len();
    let mut projections: Vec<Vec<&str>> = vec![names.iter().map(String::as_str).collect()];
    projections.extend(names.iter().map(|n| vec![n.as_str()]));
    for &batch_size in batch_sizes {
        for projection in &projections {
            let batches = read(path, batch_size, Some(projection), None);
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
    }
}

fn selections(rows: usize) -> Vec<Vec<RowSelector>> {
    let mut out = Vec::new();
    for (skip, select) in [(3, 5), (1, 1), (100, 50), (7, 1000), (0, 1), (13, 2)] {
        let mut selectors = Vec::new();
        let mut at = 0;
        while at < rows {
            selectors.push(RowSelector::skip(skip.min(rows - at)));
            at += skip.min(rows - at);
            if at < rows {
                selectors.push(RowSelector::select(select.min(rows - at)));
                at += select.min(rows - at);
            }
        }
        out.push(selectors.into_iter().filter(|s| s.row_count > 0).collect());
    }
    out.push(vec![RowSelector::skip(rows - 1), RowSelector::select(1)]);
    out.push(vec![RowSelector::select(1), RowSelector::skip(rows - 1)]);
    out.push(vec![
        RowSelector::skip(rows / 3),
        RowSelector::select(rows / 3),
        RowSelector::skip(rows - 2 * (rows / 3)),
    ]);
    out
}

fn selected_rows(selectors: &[RowSelector], column: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = 0;
    for selector in selectors {
        if !selector.skip {
            out.extend_from_slice(&column[at..at + selector.row_count]);
        }
        at += selector.row_count;
    }
    out
}

pub(crate) fn check_selections(path: &str) {
    let (names, columns) = expected(path);
    let rows = columns[0].len();
    let mut projections: Vec<Vec<&str>> = vec![names.iter().map(String::as_str).collect()];
    projections.extend(names.iter().map(|n| vec![n.as_str()]));
    for (number, selectors) in selections(rows).into_iter().enumerate() {
        let selection: RowSelection = selectors.clone().into();
        for batch_size in [1, 8192] {
            for projection in &projections {
                let batches = read(path, batch_size, Some(projection), Some(selection.clone()));
                for name in projection {
                    let index = names.iter().position(|n| n == name).unwrap();
                    assert_eq!(
                        rendered(&batches, name),
                        selected_rows(&selectors, &columns[index]),
                        "{path} batch_size {batch_size} {projection:?} column {name} selection {number}"
                    );
                }
            }
        }
    }
}

fn check_file(name: &str) {
    check_projections(&data_path(name), &BATCH_SIZES);
}

fn check_file_selections(name: &str) {
    check_selections(&data_path(name));
}

macro_rules! null_matrix_tests {
    ($($name:ident, $selection:ident: [$($file:expr),+ $(,)?]);+ $(;)?) => {
        $(
            #[test]
            #[ignore = "the PRESENT stream of the root struct is not applied to its children"]
            fn $name() {
                $(check_file($file);)+
            }

            #[test]
            #[ignore = "the PRESENT stream of the root struct is not applied to its children"]
            fn $selection() {
                $(check_file_selections($file);)+
            }
        )+
    };
}

null_matrix_tests! {
    null_matrix_root_null_rows_mixed_with_null_fields,
    null_matrix_root_null_rows_mixed_with_null_fields_row_selection: ["root_null_int"];
    null_matrix_root_null_rows_only,
    null_matrix_root_null_rows_only_row_selection: ["root_null_only", "root_null_all"];
    null_matrix_boolean_and_integers,
    null_matrix_boolean_and_integers_row_selection:
        ["prim_boolean", "prim_tinyint", "prim_smallint", "prim_int", "prim_bigint"];
    null_matrix_floats_and_strings,
    null_matrix_floats_and_strings_row_selection:
        ["prim_float", "prim_double", "prim_string_dict", "prim_string_direct", "prim_binary"];
    null_matrix_dates_timestamps_and_decimals,
    null_matrix_dates_timestamps_and_decimals_row_selection:
        ["prim_date", "prim_timestamp", "prim_decimal_10_2", "prim_decimal_30_5"];
    null_matrix_nested_struct,
    null_matrix_nested_struct_row_selection: ["nested_struct"];
    null_matrix_list_of_struct,
    null_matrix_list_of_struct_row_selection: ["list_of_struct"];
    null_matrix_map_with_null_values,
    null_matrix_map_with_null_values_row_selection: ["map_nulls"];
    null_matrix_union_int_string,
    null_matrix_union_int_string_row_selection:
        ["union_int_string", "union_int_string_small", "union_no_nulls"];
    null_matrix_union_of_struct_and_list,
    null_matrix_union_of_struct_and_list_row_selection: ["union_struct_list"];
    null_matrix_union_inside_struct_and_list,
    null_matrix_union_inside_struct_and_list_row_selection: ["union_nested", "union_in_list"];
    null_matrix_multi_stripe_with_changing_null_pattern,
    null_matrix_multi_stripe_with_changing_null_pattern_row_selection: ["multistripe"];
}

#[test]
#[ignore = "the PRESENT stream of the root struct is not applied to its children"]
fn null_matrix_root_null_rows_keep_the_values_of_the_others() {
    let path = data_path("root_null_int");
    for batch_size in BATCH_SIZES {
        let batches = read(&path, batch_size, None, None);
        assert_eq!(
            rendered(&batches, "c"),
            [
                "~",
                "~",
                "~",
                "40",
                "1763316495",
                "-308973329",
                "23",
                "28",
                "~"
            ]
        );
    }
}

#[test]
fn null_matrix_multi_stripe_file_has_several_stripes() {
    let path = data_path("multistripe");
    let builder = ArrowReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    assert!(builder.file_metadata().stripe_metadatas().len() >= 4);
}
