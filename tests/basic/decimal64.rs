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

use arrow::array::{Array, Decimal128Array, Int64Array};
use arrow::compute::concat;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

struct Expected {
    name: &'static str,
    precision: u8,
    scale: i8,
    values: &'static [i128],
}

fn read_column(file: &str, column: &str, batch_size: usize) -> Vec<Box<dyn Array>> {
    let f = File::open(basic_path(file)).unwrap();
    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let projection =
        ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &[column]);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build();
    reader
        .map(|b| b.unwrap().column(0).to_data())
        .map(|d| Box::new(arrow::array::make_array(d)) as Box<dyn Array>)
        .collect()
}

fn check(file: &str, ints: &[i64], decimals: &[Expected]) {
    for batch_size in [1, 3, 8192] {
        let parts = read_column(file, "a", batch_size);
        let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
        let joined = concat(&refs).unwrap();
        let joined = joined.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            joined.values().as_ref(),
            ints,
            "{file} a batch {batch_size}"
        );
        for e in decimals {
            let parts = read_column(file, e.name, batch_size);
            let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
            let joined = concat(&refs).unwrap();
            let joined = joined.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(
                joined.precision(),
                e.precision,
                "{file} {} precision",
                e.name
            );
            assert_eq!(joined.scale(), e.scale, "{file} {} scale", e.name);
            assert_eq!(joined.null_count(), 0);
            assert_eq!(
                joined.values().as_ref(),
                e.values,
                "{file} {} batch {batch_size}",
                e.name
            );
        }
    }
}

const DECIMAL64_V2_A: [i64; 10] = [
    17292380420,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380421,
    17292380422,
    17292380422,
    17292380422,
];

const DECIMAL64_V2_B: [i128; 10] = [24, 38, 28, 40, 2, 42, 10, 12, 41, 38];

const DECIMAL64_V2_C: [i128; 10] = [
    3616416, 6335170, 4267396, 7667760, 209648, 4528482, 1857290, 1483680, 8215252, 4724084,
];

const DECIMAL64_V2_D: [i128; 10] = [3, 8, 9, 5, 7, 7, 1, 9, 7, 10];

const DECIMAL64_V2_E: [i128; 10] = [1, 1, 6, 4, 7, 5, 8, 6, 2, 0];

#[test]
fn test_decimal64_v2_all_values() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[
            Expected {
                name: "b",
                precision: 12,
                scale: 0,
                values: &DECIMAL64_V2_B,
            },
            Expected {
                name: "c",
                precision: 20,
                scale: 2,
                values: &DECIMAL64_V2_C,
            },
            Expected {
                name: "d",
                precision: 12,
                scale: 2,
                values: &DECIMAL64_V2_D,
            },
            Expected {
                name: "e",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_E,
            },
        ],
    );
}

#[test]
fn test_decimal64_v2_column_b() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "b",
            precision: 12,
            scale: 0,
            values: &DECIMAL64_V2_B,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_c() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "c",
            precision: 20,
            scale: 2,
            values: &DECIMAL64_V2_C,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_d() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "d",
            precision: 12,
            scale: 2,
            values: &DECIMAL64_V2_D,
        }],
    );
}

#[test]
fn test_decimal64_v2_column_e() {
    check(
        "decimal64_v2.orc",
        &DECIMAL64_V2_A,
        &[Expected {
            name: "e",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_E,
        }],
    );
}

const DECIMAL64_V2_CPLUSPLUS_A: [i64; 10] = [
    17292380420,
    17292380421,
    17292380422,
    17292380423,
    17292380424,
    17292380425,
    17292380426,
    17292380427,
    17292380428,
    17292380429,
];

const DECIMAL64_V2_CPLUSPLUS_B: [i128; 10] = [
    16416, 32832, 65664, 131328, 262656, 525312, 1050624, 2101248, 4202496, 8404992,
];

const DECIMAL64_V2_CPLUSPLUS_C: [i128; 10] = [3, 8, 9, 5, 7, 7, 1, 9, 7, 12];

const DECIMAL64_V2_CPLUSPLUS_D: [i128; 10] = [1, 1, 6, 4, 7, 5, 8, 6, 2, 34];

const DECIMAL64_V2_CPLUSPLUS_E: [i128; 10] = [4, 9, 15, 9, 14, 12, 9, 15, 9, 46];

#[test]
fn test_decimal64_v2_cplusplus_all_values() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[
            Expected {
                name: "b",
                precision: 10,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_B,
            },
            Expected {
                name: "c",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_C,
            },
            Expected {
                name: "d",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_D,
            },
            Expected {
                name: "e",
                precision: 2,
                scale: 2,
                values: &DECIMAL64_V2_CPLUSPLUS_E,
            },
        ],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_b() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "b",
            precision: 10,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_B,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_c() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "c",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_C,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_d() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "d",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_D,
        }],
    );
}

#[test]
fn test_decimal64_v2_cplusplus_column_e() {
    check(
        "decimal64_v2_cplusplus.orc",
        &DECIMAL64_V2_CPLUSPLUS_A,
        &[Expected {
            name: "e",
            precision: 2,
            scale: 2,
            values: &DECIMAL64_V2_CPLUSPLUS_E,
        }],
    );
}

fn read_unscaled(file: &str, batch_size: usize) -> Vec<Vec<Option<i128>>> {
    let f = File::open(basic_path(file)).unwrap();
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_batch_size(batch_size)
        .build();
    let mut columns: Vec<Vec<Option<i128>>> = vec![];
    for batch in reader {
        let batch = batch.unwrap();
        columns.resize(batch.num_columns(), vec![]);
        for (i, column) in batch.columns().iter().enumerate() {
            let array = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
            columns[i].extend(array.iter());
        }
    }
    columns
}

fn check_pyorc(file: &str, expected: &[&[Option<i128>]]) {
    for batch_size in [1, 3, 8192] {
        let columns = read_unscaled(file, batch_size);
        assert_eq!(
            columns.len(),
            if expected[0].is_empty() {
                0
            } else {
                expected.len()
            }
        );
        for (got, want) in columns.iter().zip(expected) {
            assert_eq!(got.as_slice(), *want, "{file} batch {batch_size}");
        }
    }
}

const PYORC_DECIMAL_P18_0: [Option<i128>; 70] = [
    Some(999999999999999999),
    Some(-999999999999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(115),
    Some(-115),
    Some(28496),
    Some(-28496),
    Some(4728479),
    Some(-4728479),
    Some(1848487893),
    Some(-1848487893),
    Some(431318700173),
    Some(-431318700173),
    Some(64138746582977),
    Some(-64138746582977),
    Some(30947002485433021),
    Some(-30947002485433021),
    Some(254970937913301554),
    Some(-254970937913301554),
    None,
    Some(853667912486086265),
    None,
    Some(29773694439291284),
    None,
    None,
    None,
    None,
    None,
    Some(69760036176272577),
    None,
    Some(-461249848599817887),
    None,
    Some(509869787500745272),
    None,
    Some(-414328000095517955),
    Some(-470426491878399293),
    None,
    Some(-751422976408380910),
    None,
    None,
    None,
    Some(-134240797059115511),
    None,
    Some(556234596495686864),
    Some(-799059154745779218),
    Some(-965067620803914510),
    None,
    None,
    Some(72078284639251605),
    None,
    None,
    None,
    Some(484306021732233358),
    Some(-89530120306460736),
    None,
    Some(993018371583592586),
    None,
    None,
    Some(-508294263376461331),
    Some(-977921010836014495),
    Some(-316721612182660620),
    None,
    None,
    Some(386466141009372942),
    Some(434578452088356617),
    None,
    None,
    Some(-644588146437185835),
];

const PYORC_DECIMAL_P18_1: [Option<i128>; 70] = [
    Some(999999999999999999),
    Some(-999999999999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(58),
    Some(-58),
    Some(26925),
    Some(-26925),
    Some(5153550),
    Some(-5153550),
    Some(544182692),
    Some(-544182692),
    Some(108342229227),
    Some(-108342229227),
    Some(104613199076668),
    Some(-104613199076668),
    Some(34022397158139797),
    Some(-34022397158139797),
    Some(262681396203384658),
    Some(-262681396203384658),
    None,
    None,
    None,
    Some(-910772006712605292),
    Some(705928742905856252),
    Some(-29665989358155665),
    None,
    None,
    Some(-473353897055370487),
    None,
    None,
    None,
    Some(-51314939464915802),
    None,
    Some(-349840167940720704),
    None,
    None,
    Some(38426579494283435),
    None,
    None,
    Some(616326817860504015),
    None,
    None,
    Some(-844884625513788167),
    None,
    Some(44756163784371705),
    None,
    Some(-212291577960621080),
    Some(579839003631141790),
    Some(-820906770490118340),
    None,
    None,
    Some(527079700700949017),
    None,
    None,
    None,
    Some(-406929638529016607),
    Some(-238726204821616771),
    Some(694882579635444454),
    None,
    None,
    None,
    None,
    None,
    Some(798433367386738771),
    Some(-151561951162333320),
    None,
    Some(-608912537931382120),
    None,
];

const PYORC_DECIMAL_P18_2: [Option<i128>; 70] = [
    Some(999999999999999999),
    Some(-999999999999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(83),
    Some(-83),
    Some(13391),
    Some(-13391),
    Some(5618734),
    Some(-5618734),
    Some(546546514),
    Some(-546546514),
    Some(350892410446),
    Some(-350892410446),
    Some(113663865202468),
    Some(-113663865202468),
    Some(27329138064179572),
    Some(-27329138064179572),
    Some(266949620285466250),
    Some(-266949620285466250),
    None,
    Some(-506113121212609083),
    Some(344553617628478013),
    None,
    None,
    Some(-852105512526914035),
    None,
    Some(86862985384543016),
    Some(-506364269229980777),
    Some(442843347896937528),
    Some(-272802501358659470),
    None,
    Some(-904334273796591130),
    Some(-183236469279840039),
    None,
    Some(-686679384224718593),
    None,
    Some(913085162584483553),
    None,
    None,
    Some(-748616528615141293),
    None,
    Some(899640641190103582),
    None,
    None,
    Some(-477826602756641949),
    None,
    None,
    Some(-53492454305865907),
    Some(38789067170498258),
    None,
    Some(268243787599455585),
    None,
    None,
    Some(-441312946732754216),
    Some(-797449316634617499),
    None,
    None,
    Some(16648935562914743),
    Some(-28278443078805927),
    None,
    None,
    Some(229295562994675373),
    None,
    Some(-657177516593463235),
    None,
    Some(-489453690921566311),
    Some(288447361648126369),
    None,
];

#[test]
fn test_pyorc_decimal_p18() {
    check_pyorc(
        "pyorc_decimal_p18.orc",
        &[
            &PYORC_DECIMAL_P18_0,
            &PYORC_DECIMAL_P18_1,
            &PYORC_DECIMAL_P18_2,
        ],
    );
}

const PYORC_DECIMAL_P1_0: [Option<i128>; 12] = [
    Some(9),
    Some(-9),
    Some(0),
    Some(1),
    Some(-1),
    Some(8),
    Some(-8),
    None,
    Some(7),
    Some(0),
    Some(-3),
    Some(-5),
];

const PYORC_DECIMAL_P1_1: [Option<i128>; 12] = [
    Some(9),
    Some(-9),
    Some(0),
    Some(1),
    Some(-1),
    Some(9),
    Some(-9),
    None,
    None,
    None,
    None,
    None,
];

#[test]
fn test_pyorc_decimal_p1() {
    check_pyorc(
        "pyorc_decimal_p1.orc",
        &[&PYORC_DECIMAL_P1_0, &PYORC_DECIMAL_P1_1],
    );
}

const PYORC_DECIMAL_P9_0: [Option<i128>; 40] = [
    Some(999999999),
    Some(-999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(11),
    Some(-11),
    Some(17036),
    Some(-17036),
    Some(7728087),
    Some(-7728087),
    Some(438512335),
    Some(-438512335),
    None,
    None,
    None,
    None,
    Some(-858480338),
    Some(-111426915),
    None,
    Some(16106586),
    Some(-727339177),
    Some(-68684469),
    Some(127389799),
    None,
    None,
    Some(987003652),
    None,
    None,
    None,
    Some(-974589401),
    Some(-308333639),
    Some(-475011844),
    Some(-774760083),
    None,
    Some(829481427),
    None,
    Some(506175363),
    Some(-950070985),
    Some(359371339),
];

const PYORC_DECIMAL_P9_1: [Option<i128>; 40] = [
    Some(999999999),
    Some(-999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(40),
    Some(-40),
    Some(17763),
    Some(-17763),
    Some(1746277),
    Some(-1746277),
    Some(935928731),
    Some(-935928731),
    None,
    None,
    None,
    None,
    Some(-500369905),
    None,
    Some(-321666183),
    None,
    None,
    None,
    None,
    None,
    Some(-442733328),
    Some(-791659993),
    Some(787068315),
    None,
    Some(-569116359),
    None,
    None,
    Some(223851035),
    Some(957158545),
    Some(-752158389),
    None,
    None,
    None,
    None,
    Some(636376525),
];

const PYORC_DECIMAL_P9_2: [Option<i128>; 40] = [
    Some(999999999),
    Some(-999999999),
    Some(0),
    Some(1),
    Some(-1),
    Some(37),
    Some(-37),
    Some(6339),
    Some(-6339),
    Some(1580770),
    Some(-1580770),
    Some(131812004),
    Some(-131812004),
    None,
    None,
    None,
    None,
    None,
    Some(-711270271),
    None,
    Some(-298143780),
    Some(2788676),
    Some(106570684),
    None,
    Some(290486921),
    None,
    None,
    Some(-610727382),
    Some(-25147682),
    Some(587708186),
    None,
    None,
    None,
    None,
    Some(721650872),
    None,
    Some(82951696),
    Some(728340799),
    None,
    None,
];

#[test]
fn test_pyorc_decimal_p9() {
    check_pyorc(
        "pyorc_decimal_p9.orc",
        &[
            &PYORC_DECIMAL_P9_0,
            &PYORC_DECIMAL_P9_1,
            &PYORC_DECIMAL_P9_2,
        ],
    );
}

const PYORC_DECIMAL_ONE_ROW_0: [Option<i128>; 1] = [Some(999999999999)];

#[test]
fn test_pyorc_decimal_one_row() {
    check_pyorc("pyorc_decimal_one_row.orc", &[&PYORC_DECIMAL_ONE_ROW_0]);
}

const PYORC_DECIMAL_EMPTY_0: [Option<i128>; 0] = [];

#[test]
fn test_pyorc_decimal_empty() {
    check_pyorc("pyorc_decimal_empty.orc", &[&PYORC_DECIMAL_EMPTY_0]);
}
