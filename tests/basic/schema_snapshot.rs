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

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use orc_rust::reader::metadata::read_metadata;

const SNAPSHOT: &str = "tests/basic/data/type_tree/schema_snapshot.tsv";
const UPDATE_VARIABLE: &str = "ORC_UPDATE_SCHEMA_SNAPSHOT";

fn collect_orc_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_orc_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "orc") {
            out.push(path);
        }
    }
}

fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ byte as u64).wrapping_mul(0x100000001b3)
    })
}

fn current_snapshot() -> BTreeMap<String, String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![];
    collect_orc_files(&root.join("tests"), &mut files);
    files
        .into_iter()
        .map(|path| {
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let mut file = File::open(&path).unwrap();
            let outcome = match std::panic::catch_unwind(move || read_metadata(&mut file)) {
                Err(_) => "panic".to_string(),
                Ok(Ok(metadata)) => {
                    let schema = format!("{:?}", metadata.root_data_type().children());
                    format!("{:016x}", fnv1a(&schema))
                }
                Ok(Err(_)) => "error".to_string(),
            };
            (name, outcome)
        })
        .collect()
}

#[test]
fn test_committed_file_schemas_are_unchanged() {
    let actual = current_snapshot();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT);
    if std::env::var_os(UPDATE_VARIABLE).is_some() {
        let text = actual
            .iter()
            .map(|(name, outcome)| format!("{name}\t{outcome}\n"))
            .collect::<String>();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| {
            let (name, outcome) = line.split_once('\t').unwrap();
            (name.to_string(), outcome.to_string())
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
}
