# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Writes ORC files with empty struct columns using pyarrow together with the rows
pyarrow reads back, to be checked by tests/basic/empty_struct.rs.

usage: generate_orc_empty_struct.py OUT_DIR COUNT SEED [--fixtures]

Every case is OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected. The first line of the
expected file holds the column names separated by tabs, every other line is one
row with one rendered cell per column, separated by tabs. A cell is "~" for
null, the decimal integer, "T" or "F" for a boolean, the string itself, "{a=X,b=Y}"
for a struct and "[X,Y]" for a list. With --fixtures the named hand written cases
are written as well, with the random cases limited to 150 rows to keep them small;
COUNT random cases are always written as prop_NNN.
"""

import os
import random
import sys

import pyarrow as pa
from pyarrow import orc

COMPRESSIONS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]


def render(v):
    if v is None:
        return "~"
    if isinstance(v, bool):
        return "T" if v else "F"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}={render(x)}" for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "[" + ",".join(render(x) for x in v) + "]"
    return str(v)


def write_case(out_dir, name, table, **options):
    path = os.path.join(out_dir, name + ".orc")
    orc.write_table(table, path, **options)
    back = orc.read_table(path)
    assert back.schema.equals(table.schema), (back.schema, table.schema)
    names = back.column_names
    columns = [back.column(n).to_pylist() for n in names]
    with open(os.path.join(out_dir, name + ".expected"), "w") as f:
        f.write("\t".join(names) + "\n")
        for i in range(back.num_rows):
            f.write("\t".join(render(c[i]) for c in columns) + "\n")
    return orc.ORCFile(path).nstripes


def hand_written(out_dir):
    empty = pa.struct([])
    write_case(
        out_dir,
        "es",
        pa.table({"id": [1, 2, 3], "a": pa.array([{}, None, {}], empty)}),
    )
    write_case(
        out_dir,
        "es_list",
        pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "l": pa.array(
                    [[{}, {}], None, [], [{}, None, {}], [None]], pa.list_(empty)
                ),
                "s": pa.array(["a", "b", "c", "d", "e"]),
            }
        ),
    )
    nested = pa.struct([("k", empty), ("n", pa.int64())])
    write_case(
        out_dir,
        "es_nested",
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "s": pa.array(
                    [{"k": {}, "n": 7}, None, {"k": None, "n": 8}, {"k": {}, "n": None}],
                    nested,
                ),
                "only": pa.array([{"k": {}}, None, {"k": None}, {"k": {}}], pa.struct([("k", empty)])),
            }
        ),
    )
    rng = random.Random(7)
    rows = 2000
    stripes = write_case(
        out_dir,
        "es_multistripe",
        pa.table(
            {
                "id": list(range(rows)),
                "a": pa.array([None if rng.random() < 0.3 else {} for _ in range(rows)], empty),
                "l": pa.array(
                    [
                        None if rng.random() < 0.2 else [{} if rng.random() < 0.7 else None for _ in range(rng.randint(0, 3))]
                        for _ in range(rows)
                    ],
                    pa.list_(empty),
                ),
            }
        ),
        stripe_size=1,
        batch_size=500,
        compression="uncompressed",
    )
    assert stripes > 1, stripes


def random_type(rng, depth):
    choices = ["int", "str", "bool", "empty"]
    if depth > 0:
        choices += ["struct", "struct", "list", "list"]
    kind = rng.choice(choices)
    if kind == "int":
        return pa.int64()
    if kind == "str":
        return pa.string()
    if kind == "bool":
        return pa.bool_()
    if kind == "empty":
        return pa.struct([])
    if kind == "struct":
        n = rng.randint(0, 3)
        return pa.struct([(f"f{i}", random_type(rng, depth - 1)) for i in range(n)])
    return pa.list_(random_type(rng, depth - 1))


def random_value(rng, typ, null_ratio):
    if rng.random() < null_ratio:
        return None
    if pa.types.is_int64(typ):
        return rng.randint(-1000, 1000)
    if pa.types.is_string(typ):
        return "s" + str(rng.randint(0, 99))
    if pa.types.is_boolean(typ):
        return rng.random() < 0.5
    if pa.types.is_struct(typ):
        return {typ[i].name: random_value(rng, typ[i].type, null_ratio) for i in range(typ.num_fields)}
    return [random_value(rng, typ.value_type, null_ratio) for _ in range(rng.randint(0, 3))]


def random_case(out_dir, name, rng, max_rows):
    rows = min(max_rows, rng.choice([0, 1, 2, 7, rng.randint(0, 200), rng.randint(0, 5000), 5000]))
    null_ratio = rng.choice([0.0, 0.0, 0.1, 0.5, 0.9, 1.0, rng.random()])
    arrays = {"id": pa.array(list(range(rows)), pa.int64())}
    for i in range(rng.randint(1, 4)):
        typ = random_type(rng, 3)
        if i == 0 and rng.random() < 0.6:
            typ = rng.choice([pa.struct([]), pa.list_(pa.struct([])), pa.struct([("k", pa.struct([]))])])
        arrays[f"c{i}"] = pa.array([random_value(rng, typ, null_ratio) for _ in range(rows)], typ)
    return write_case(
        out_dir,
        name,
        pa.table(arrays),
        compression=rng.choice(COMPRESSIONS),
        stripe_size=rng.choice([2048, 16384, 64 * 1024 * 1024]),
        compression_block_size=rng.choice([65536, 262144]),
        row_index_stride=rng.choice([100, 10000]),
        batch_size=rng.choice([100, 1024]),
    )


def main():
    out_dir, count, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    os.makedirs(out_dir, exist_ok=True)
    if "--fixtures" in sys.argv:
        hand_written(out_dir)
    max_rows = 150 if "--fixtures" in sys.argv else 5000
    rng = random.Random(seed)
    for n in range(count):
        random_case(out_dir, f"prop_{n:03}", rng, max_rows)


if __name__ == "__main__":
    main()
