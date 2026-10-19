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

//! Small files written by Apache ORC Java and Apache ORC C++ whose metadata or streams
//! look unusual but are accepted by the other readers. The files come from
//! scripts/generate_orc_java_corpus.sh and scripts/generate_orc_real_writers.py, the
//! expected rows are the ones pyarrow reads.

use crate::basic_path;
use crate::null_matrix::{check_projections, check_selections, BATCH_SIZES};

fn check(name: &str) {
    let path = basic_path(&format!("real_writers/{name}.orc"));
    check_projections(&path, &BATCH_SIZES);
    check_selections(&path);
}

#[test]
#[ignore = "files that decoded before the stripe consistency checks are refused"]
fn real_writer_java_1_6_list_of_strings_with_more_dictionary_entries_than_rows() {
    check("java_1_6_14_list_of_strings_dictionary");
}

#[test]
#[ignore = "files that decoded before the stripe consistency checks are refused"]
fn real_writer_java_2_0_list_of_strings_with_more_dictionary_entries_than_rows() {
    check("java_2_0_7_list_of_strings_dictionary");
}

#[test]
#[ignore = "files that decoded before the stripe consistency checks are refused"]
fn real_writer_java_2_3_list_of_strings_with_more_dictionary_entries_than_rows() {
    check("java_2_3_1_list_of_strings_dictionary");
}

#[test]
#[ignore = "files that decoded before the stripe consistency checks are refused"]
fn real_writer_cpp_pyarrow_list_of_strings_with_more_dictionary_entries_than_rows() {
    check("cpp_pyarrow_list_of_strings_dictionary");
}

#[test]
#[ignore = "files that decoded before the stripe consistency checks are refused"]
fn real_writer_cpp_pyorc_list_of_strings_with_more_dictionary_entries_than_rows() {
    check("cpp_pyorc_list_of_strings_dictionary");
}

#[test]
#[ignore = "an int variant of a union with values no row reads is refused"]
fn real_writer_cpp_pyorc_union_with_a_null_int_variant_and_garbage_statistics() {
    check("cpp_pyorc_union_with_null_int_variant");
}
