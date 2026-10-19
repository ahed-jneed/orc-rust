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

//! Prints the rows of an ORC file the way tests/basic/null_matrix.rs renders them, for
//! comparing what this reader decodes with what another reader decodes.
//!
//! `dump_rows <file> <batch-size> [column]...` prints the column names, tab separated, then
//! one line per row. An error while decoding is printed as a last line starting with `ERROR:`.

use std::fs::File;

use arrow::array::{Array, AsArray, UnionArray};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatchReader;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

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
            let cells: Vec<String> = fields
                .iter()
                .zip(a.columns())
                .map(|(f, c)| format!("{}={}", f.name(), render(c, row)))
                .collect();
            format!("{{{}}}", cells.join(","))
        }
        DataType::List(_) => {
            let v = array.as_list::<i32>().value(row);
            let cells: Vec<String> = (0..v.len()).map(|i| render(&v, i)).collect();
            format!("[{}]", cells.join(","))
        }
        DataType::Map(..) => {
            let v = array.as_map().value(row);
            let cells: Vec<String> = (0..v.len())
                .map(|i| format!("{}={}", render(v.column(0), i), render(v.column(1), i)))
                .collect();
            format!("{{{}}}", cells.join(","))
        }
        DataType::Union(..) => {
            let a = array.as_any().downcast_ref::<UnionArray>().unwrap();
            let tag = a.type_id(row);
            let child = a.child(tag);
            let off = a.value_offset(row);
            if child.is_null(off) {
                "~".to_string()
            } else {
                format!("<{}:{}>", tag, render(child, off))
            }
        }
        other => panic!("unexpected type {other}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    let batch_size: usize = args[2].parse().unwrap();
    let builder = ArrowReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .with_batch_size(batch_size);
    let builder = if args.len() > 3 {
        let names: Vec<&str> = args[3..].iter().map(String::as_str).collect();
        let mask = ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &names);
        builder.with_projection(mask)
    } else {
        builder
    };
    let reader = builder.build();
    let names: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut out = format!("{}\n", names.join("\t"));
    for batch in reader {
        match batch {
            Ok(batch) => {
                for row in 0..batch.num_rows() {
                    let cells: Vec<String> =
                        batch.columns().iter().map(|c| render(c, row)).collect();
                    out.push_str(&cells.join("\t"));
                    out.push('\n');
                }
            }
            Err(e) => {
                out.push_str(&format!("ERROR: {e}\n"));
                break;
            }
        }
    }
    print!("{out}");
}
