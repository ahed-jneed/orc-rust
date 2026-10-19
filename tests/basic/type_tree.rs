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

use bytes::Bytes;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::proto::r#type::Kind;
use orc_rust::proto::{Footer, PostScript, Type};
use orc_rust::schema::MAX_TYPE_TREE_DEPTH;
use prost::Message;

fn node(kind: Kind, subtypes: &[u32], field_names: &[&str]) -> Type {
    Type {
        kind: Some(kind as i32),
        subtypes: subtypes.to_vec(),
        field_names: field_names.iter().map(|name| name.to_string()).collect(),
        ..Default::default()
    }
}

fn leaf() -> Type {
    node(Kind::Int, &[], &[])
}

fn structure(subtypes: &[u32]) -> Type {
    let names = vec!["c"; subtypes.len()];
    node(Kind::Struct, subtypes, &names)
}

fn orc_file(types: Vec<Type>) -> Bytes {
    let footer = Footer {
        header_length: Some(3),
        content_length: Some(0),
        types,
        number_of_rows: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    let postscript = PostScript {
        footer_length: Some(footer.len() as u64),
        compression: Some(0),
        version: vec![0, 12],
        metadata_length: Some(0),
        magic: Some("ORC".to_string()),
        ..Default::default()
    }
    .encode_to_vec();
    let mut file = b"ORC".to_vec();
    file.extend_from_slice(&footer);
    file.extend_from_slice(&postscript);
    file.push(postscript.len() as u8);
    Bytes::from(file)
}

fn open(types: Vec<Type>) -> Result<String, String> {
    ArrowReaderBuilder::try_new(orc_file(types))
        .map(|builder| builder.file_metadata().root_data_type().to_string())
        .map_err(|error| error.to_string())
}

fn assert_refused(types: Vec<Type>, expected: &str) {
    let error = open(types).expect_err("malformed type tree must be refused");
    assert!(error.contains(expected), "{error}");
}

fn chain(depth: usize) -> Vec<Type> {
    let mut types = (0..depth)
        .map(|index| structure(&[index as u32 + 1]))
        .collect::<Vec<_>>();
    types.push(leaf());
    types
}

#[test]
fn test_root_that_is_not_a_struct_is_refused() {
    assert_refused(vec![leaf()], "Root type");
    assert_refused(vec![node(Kind::List, &[1], &[]), leaf()], "Root type");
    assert_refused(
        vec![node(Kind::Map, &[1, 2], &[]), leaf(), leaf()],
        "Root type",
    );
    assert_refused(vec![node(Kind::Union, &[1], &[]), leaf()], "Root type");
}

#[test]
fn test_struct_naming_itself_as_child_is_refused() {
    assert_refused(vec![structure(&[0])], "refers");
    assert_refused(vec![structure(&[1]), structure(&[1])], "refers");
}

#[test]
fn test_subtype_list_referring_to_an_earlier_entry_is_refused() {
    assert_refused(vec![structure(&[1]), structure(&[0])], "refers");
    assert_refused(
        vec![structure(&[1]), structure(&[2]), structure(&[1])],
        "refers",
    );
    assert_refused(vec![structure(&[1]), node(Kind::List, &[0], &[])], "refers");
    assert_refused(
        vec![structure(&[1]), node(Kind::Map, &[2, 1], &[]), leaf()],
        "refers",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Union, &[2, 0], &[]), leaf()],
        "refers",
    );
}

#[test]
fn test_subtype_index_past_the_list_is_refused() {
    assert_refused(vec![structure(&[1])], "out of bounds");
    assert_refused(vec![structure(&[2]), leaf()], "out of bounds");
    assert_refused(vec![structure(&[u32::MAX])], "out of bounds");
    assert_refused(
        vec![structure(&[1]), node(Kind::List, &[2], &[])],
        "out of bounds",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Map, &[2, 9], &[]), leaf()],
        "out of bounds",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Union, &[2, 3], &[]), leaf()],
        "out of bounds",
    );
}

#[test]
fn test_node_with_two_parents_is_refused() {
    assert_refused(vec![structure(&[1, 1]), leaf()], "more than once");
    assert_refused(
        vec![
            structure(&[1, 2]),
            node(Kind::List, &[3], &[]),
            node(Kind::List, &[3], &[]),
            leaf(),
        ],
        "more than once",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Map, &[2, 2], &[]), leaf()],
        "more than once",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Union, &[2, 2], &[]), leaf()],
        "more than once",
    );
}

#[test]
fn test_shared_subtrees_cannot_multiply_the_tree() {
    let rungs = 200;
    let mut types = vec![structure(&[1])];
    for rung in 0..rungs {
        let next = 2 * rung + 3;
        types.push(structure(&[next, next]));
    }
    types.push(leaf());
    assert_refused(types, "more than once");
}

#[test]
fn test_tree_deeper_than_the_limit_is_refused() {
    assert_refused(chain(MAX_TYPE_TREE_DEPTH + 1), "deeper");
    assert_refused(chain(1_000_000), "deeper");
}

#[test]
fn test_tree_at_the_depth_limit_is_read() {
    let schema = open(chain(MAX_TYPE_TREE_DEPTH)).unwrap();
    assert_eq!(schema.matches("STRUCT").count(), MAX_TYPE_TREE_DEPTH - 1);
}

#[test]
fn test_field_name_count_not_matching_subtype_count_is_refused() {
    let mut types = vec![structure(&[1, 2]), leaf(), leaf()];
    types[0].field_names.pop();
    assert_refused(types, "matching lengths");
    let mut types = vec![structure(&[1]), leaf()];
    types[0].field_names.push("extra".to_string());
    assert_refused(types, "matching lengths");
    let mut types = vec![structure(&[1]), structure(&[2]), leaf()];
    types[1].field_names.clear();
    assert_refused(types, "matching lengths");
}

#[test]
fn test_list_and_map_with_wrong_subtype_counts_are_refused() {
    assert_refused(
        vec![structure(&[1]), node(Kind::List, &[], &[])],
        "1 sub type",
    );
    assert_refused(
        vec![
            structure(&[1]),
            node(Kind::List, &[2, 3], &[]),
            leaf(),
            leaf(),
        ],
        "1 sub type",
    );
    assert_refused(
        vec![structure(&[1]), node(Kind::Map, &[2], &[]), leaf()],
        "2 sub types",
    );
    assert_refused(
        vec![
            structure(&[1]),
            node(Kind::Map, &[2, 3, 4], &[]),
            leaf(),
            leaf(),
            leaf(),
        ],
        "2 sub types",
    );
}

#[test]
fn test_union_variant_count_limit() {
    let build = |variants: u32| {
        let subtypes = (2..2 + variants).collect::<Vec<_>>();
        let mut types = vec![structure(&[1]), node(Kind::Union, &subtypes, &[])];
        types.extend((0..variants).map(|_| leaf()));
        types
    };
    assert!(open(build(127)).is_ok());
    assert_refused(build(128), "127 variants");
}

#[test]
fn test_empty_type_list_is_refused() {
    assert!(open(vec![]).is_err());
}

#[test]
fn test_unreferenced_types_are_still_read() {
    let schema = open(vec![structure(&[1]), leaf(), leaf()]).unwrap();
    assert_eq!(schema, "ROOT\n  c INTEGER");
}

#[test]
fn test_nested_compound_types_are_read() {
    let types = vec![
        node(Kind::Struct, &[1, 3, 7], &["list", "map", "union"]),
        node(Kind::List, &[2], &[]),
        leaf(),
        node(Kind::Map, &[4, 5], &[]),
        leaf(),
        node(Kind::Struct, &[6], &["inner"]),
        leaf(),
        node(Kind::Union, &[8, 9], &[]),
        leaf(),
        node(Kind::List, &[10], &[]),
        leaf(),
    ];
    let schema = open(types).unwrap();
    assert_eq!(
        schema,
        "ROOT\n  list LIST\n  INTEGER\n  map MAP\n  INTEGER\n  STRUCT\n  inner INTEGER\n  union UNION\n  INTEGER\n  LIST\n  INTEGER"
    );
}

#[test]
fn test_types_that_nothing_refers_to_are_not_checked() {
    let mut orphan = structure(&[0]);
    orphan.field_names.clear();
    let schema = open(vec![structure(&[1]), leaf(), orphan, structure(&[9])]).unwrap();
    assert_eq!(schema, "ROOT\n  c INTEGER");
}

#[test]
fn test_depth_limit_is_128_levels_below_the_root() {
    assert_eq!(MAX_TYPE_TREE_DEPTH, 128);
    assert!(open(chain(128)).is_ok());
    assert_refused(chain(129), "deeper");
}

const LEAF_KINDS: [Kind; 15] = [
    Kind::Boolean,
    Kind::Byte,
    Kind::Short,
    Kind::Int,
    Kind::Long,
    Kind::Float,
    Kind::Double,
    Kind::String,
    Kind::Binary,
    Kind::Timestamp,
    Kind::Decimal,
    Kind::Date,
    Kind::Varchar,
    Kind::Char,
    Kind::TimestampInstant,
];

#[test]
fn test_leaf_types_without_subtypes_are_read() {
    for kind in LEAF_KINDS {
        assert!(open(vec![structure(&[1]), node(kind, &[], &[])]).is_ok());
    }
}

#[test]
fn test_leaf_type_with_subtypes_is_refused() {
    for kind in LEAF_KINDS {
        assert_refused(
            vec![structure(&[1]), node(kind, &[2], &[]), leaf()],
            "cannot have subtypes or field names",
        );
        assert_refused(
            vec![structure(&[1]), node(kind, &[2, 3], &[]), leaf(), leaf()],
            "cannot have subtypes or field names",
        );
    }
}

#[test]
fn test_leaf_type_with_field_names_is_refused() {
    for kind in LEAF_KINDS {
        assert_refused(
            vec![structure(&[1]), node(kind, &[], &["x"])],
            "cannot have subtypes or field names",
        );
    }
}
