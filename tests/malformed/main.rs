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

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use orc_rust::proto::PostScript;
use orc_rust::reader::metadata::read_metadata;
use orc_rust::ArrowWriterBuilder;
use prost::Message;

fn file_with_postscript(edit: impl FnOnce(&mut PostScript)) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut file = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut file, schema)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript_start = file.len() - 1 - *file.last().unwrap() as usize;
    let mut postscript = PostScript::decode(&file[postscript_start..file.len() - 1]).unwrap();
    edit(&mut postscript);
    let postscript = postscript.encode_to_vec();
    file.truncate(postscript_start);
    file.extend_from_slice(&postscript);
    file.push(postscript.len() as u8);
    Bytes::from(file)
}

#[test]
fn a_footer_longer_than_the_file_is_an_error() {
    let mut file = file_with_postscript(|postscript| postscript.footer_length = Some(1 << 40));
    assert!(read_metadata(&mut file).is_err());
}

#[test]
fn footer_and_metadata_lengths_that_overflow_are_an_error() {
    let mut file = file_with_postscript(|postscript| {
        postscript.footer_length = Some(u64::MAX);
        postscript.metadata_length = Some(u64::MAX);
    });
    assert!(read_metadata(&mut file).is_err());
}
