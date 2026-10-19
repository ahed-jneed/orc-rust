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
Writes random integer columns in file versions 0.11 (RLE v1) and 0.12 (RLE v2)
with pyarrow, together with the values pyarrow reads back, to be checked by
tests/basic/rle_v1_wrapping.rs.

usage: generate_orc_rle_v1_property.py OUT_DIR COUNT SEED [--fixtures]

Every case is OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected, whose first line is
the type of column "v" (bigint, int, smallint or tinyint) and whose other lines
are its values, or "null". Column "x" holds the row number.
"""

import os
import random
import sys

import pyarrow as pa
from pyarrow import orc

TYPES = {
    "bigint": (64, pa.int64()),
    "int": (32, pa.int32()),
    "smallint": (16, pa.int16()),
    "tinyint": (8, pa.int8()),
}
COMPRESSIONS = ["uncompressed", "zlib", "snappy", "zstd"]
FIXED_DELTAS = [0, 1, -1, 127, -128, 126, -127, 2, -2]


def wrap(value, bits):
    value &= (1 << bits) - 1
    return value - (1 << bits) if value >> (bits - 1) else value


def random_delta(rng, bits):
    roll = rng.random()
    if roll < 0.5:
        return rng.choice(FIXED_DELTAS)
    if roll < 0.7:
        return rng.randint(-128, 127)
    return wrap(rng.getrandbits(bits), bits)


def random_start(rng, bits):
    low, high = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    roll = rng.random()
    if roll < 0.3:
        return low + rng.randint(0, 300)
    if roll < 0.6:
        return high - rng.randint(0, 300)
    if roll < 0.7:
        return rng.randint(-300, 300)
    return rng.randint(low, high)


def random_values(rng, bits, rows):
    values = []
    while len(values) < rows:
        start = random_start(rng, bits)
        delta = random_delta(rng, bits)
        length = rng.randint(3, 130)
        repeats = rng.choice([1, 1, 1, 2, 3])
        for _ in range(repeats):
            values.extend(wrap(start + i * delta, bits) for i in range(length))
        if rng.random() < 0.2:
            values.extend(wrap(rng.getrandbits(bits), bits) for _ in range(rng.randint(1, 5)))
    return values[:rows]


def with_nulls(rng, values, ratio):
    return [None if rng.random() < ratio else v for v in values]


def write_case(out_dir, name, kind, values, version, compression):
    path = os.path.join(out_dir, name + ".orc")
    table = pa.table(
        {
            "v": pa.array(values, TYPES[kind][1]),
            "x": pa.array(range(len(values)), pa.int64()),
        }
    )
    orc.write_table(table, path, file_version=version, compression=compression)
    expected = orc.read_table(path).column("v").to_pylist()
    assert expected == values, name
    with open(os.path.join(out_dir, name + ".expected"), "w") as f:
        f.write(kind + "\n")
        for v in expected:
            f.write("null\n" if v is None else f"{v}\n")


def main():
    out_dir, count, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    fixtures = "--fixtures" in sys.argv
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(seed)
    kinds = list(TYPES)
    if fixtures:
        minimal = [-(1 << 63), (1 << 63) - 1, (1 << 63) - 2]
        write_case(out_dir, "minimal_bigint_v011", "bigint", minimal, "0.11", "uncompressed")
    for i in range(count):
        kind = kinds[i % len(kinds)]
        version = "0.11" if (i // len(kinds)) % 2 == 0 else "0.12"
        bits = TYPES[kind][0]
        rows = rng.randint(1, 40) if fixtures else rng.choice([5, 130, 700, 3000, 12000])
        values = random_values(rng, bits, rows)
        if rng.random() < 0.5:
            values = with_nulls(rng, values, rng.choice([0.05, 0.3]))
        compression = "uncompressed" if fixtures else rng.choice(COMPRESSIONS)
        name = f"case_{i:04d}_{kind}_v{version.replace('.', '')}"
        write_case(out_dir, name, kind, values, version, compression)


main()
