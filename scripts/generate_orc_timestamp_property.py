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
Writes random timestamp columns with pyarrow and pyorc together with the values
the reference readers return, to be checked by tests/basic/timestamp_secondary.rs.

usage: generate_orc_timestamp_property.py OUT_DIR COUNT SEED [--fixtures]

Every case is OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected, whose first line is
"UNIT TZ" (UNIT is us or ns, TZ is none or utc) and whose other lines are the
values in that unit, or "null". The expected values are what pyarrow reads, or
what pyorc reads where pyarrow cannot represent the range in nanoseconds; both
are checked against the written values.
"""

import random
import sys
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyarrow import orc
import pyorc

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ORC_EPOCH_NS = 1_420_070_400 * 1_000_000_000
US_MIN = int((datetime(1000, 1, 1, tzinfo=timezone.utc) - EPOCH) / timedelta(microseconds=1))
US_MAX = int((datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc) - EPOCH) / timedelta(microseconds=1))
PYARROW_COMPRESSIONS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]
PYORC_COMPRESSIONS = [
    pyorc.CompressionKind.NONE,
    pyorc.CompressionKind.ZLIB,
    pyorc.CompressionKind.SNAPPY,
    pyorc.CompressionKind.LZ4,
    pyorc.CompressionKind.ZSTD,
]


def random_values(rng, unit, rows, low, high):
    digits = 9 if unit == "ns" else 6
    weights = [rng.random() ** 2 for _ in range(digits + 1)]
    scale = 1 if unit == "ns" else 1000
    out = []
    for _ in range(rows):
        d = rng.choices(range(digits + 1), weights)[0]
        seconds = rng.randint(low // 1_000_000_000 + 1, high // 1_000_000_000 - 1)
        fraction = rng.randrange(10**d) * 10 ** (9 - d) if d else 0
        if rng.random() < 0.05:
            fraction = rng.choice([1, 10, 100, 1000, 10**6, 10**8, 999_999_999, 500_000_000])
        nanos = seconds * 1_000_000_000 + fraction
        out.append(nanos // scale if unit == "us" else nanos)
    return out


def with_nulls(rng, values, ratio):
    return [None if rng.random() < ratio else v for v in values]


def to_datetime(us):
    return EPOCH + timedelta(microseconds=us)


def from_datetime(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - EPOCH) // timedelta(microseconds=1)


def write_pyarrow(path, values, unit, instant, rng):
    typ = pa.timestamp(unit, "UTC" if instant else None)
    array = pa.array(values, pa.int64()).cast(typ)
    orc.write_table(
        pa.table({"t": array}),
        path,
        compression=rng.choice(PYARROW_COMPRESSIONS),
        stripe_size=rng.choice([1024, 16384, 64 * 1024 * 1024]),
        compression_block_size=rng.choice([65536, 262144]),
        row_index_stride=rng.choice([100, 10000]),
    )


def write_pyorc(path, values_us, instant, rng):
    desc = "struct<t:timestamp with local time zone>" if instant else "struct<t:timestamp>"
    with open(path, "wb") as f:
        with pyorc.Writer(
            f,
            desc,
            compression=rng.choice(PYORC_COMPRESSIONS),
            stripe_size=rng.choice([4096, 65536, 64 * 1024 * 1024]),
            compression_block_size=rng.choice([65536, 262144]),
            row_index_stride=rng.choice([100, 10000]),
        ) as writer:
            for v in values_us:
                writer.write((None if v is None else to_datetime(v),))


def read_pyarrow(path):
    column = orc.read_table(path).column("t").combine_chunks().cast(pa.int64())
    return column.to_pylist()


def read_pyorc(path):
    with open(path, "rb") as f:
        return [None if row[0] is None else from_datetime(row[0]) for row in pyorc.Reader(f)]


def oracle(path, written, writer):
    try:
        return "ns", read_pyarrow(path), 0
    except pa.ArrowInvalid:
        read = read_pyorc(path)
        disagree = sum(1 for a, b in zip(written, read) if a != b)
        return "us", read if writer == "pyorc" else written, disagree


def save(out, name, unit, instant, values):
    with open(f"{out}/{name}.expected", "w") as f:
        f.write(f"{unit} {'utc' if instant else 'none'}\n")
        for v in values:
            f.write("null\n" if v is None else f"{v}\n")


def check_and_save(out, name, path, written, written_unit, instant):
    unit, read, disagree = oracle(path, written, "pyorc" if "_pyorc_" in name else "pyarrow")
    assert unit == "ns" or written_unit == "us", name
    if unit == "ns" and written_unit == "us":
        written = [None if v is None else v * 1000 for v in written]
    mismatches = sum(1 for a, b in zip(written, read) if a != b)
    save(out, name, unit, instant, read)
    return unit, mismatches, disagree


def random_case(rng, rows=None):
    kind = rng.choice(["ns", "us", "us", "us_ns_range"])
    instant = rng.random() < 0.3
    rows = rows if rows is not None else int(10 ** rng.uniform(0, 4.3))
    rows = max(1, min(rows, 20000))
    ratio = rng.choice([0, 0, 0.05, 0.2, 0.5, rng.uniform(0, 0.5)])
    if kind == "ns":
        low, high, unit = 1700, 2262, "ns"
    elif kind == "us":
        low, high, unit = 1000, 9999, "us"
    else:
        low, high, unit = 1700, 2262, "us"
    lo = -8_520_336_000 * 1_000_000_000 if low == 1700 else US_MIN * 1000
    hi = 9_214_646_400 * 1_000_000_000 if high == 2262 else US_MAX * 1000
    values = with_nulls(rng, random_values(rng, unit, rows, lo, hi), ratio)
    return kind, unit, instant, values


def record(report, out, name, path, written, written_unit, instant):
    read_unit, mismatches, disagree = check_and_save(out, name, path, written, written_unit, instant)
    report["cases"] += 1
    report["mismatches"] += mismatches
    report["oracle_disagrees_with_written"] += disagree
    report["pyarrow_oracle" if read_unit == "ns" else "written_oracle"] += 1


def main():
    out, count, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    fixtures = "--fixtures" in sys.argv
    rng = random.Random(seed)
    report = {
        "cases": 0,
        "mismatches": 0,
        "oracle_disagrees_with_written": 0,
        "pyarrow_oracle": 0,
        "written_oracle": 0,
    }
    for i in range(count):
        kind, unit, instant, values = random_case(rng, rng.randint(20, 120) if fixtures else None)
        suffix = "_instant" if instant else ""
        name = f"case_{i:04d}_pyarrow_{kind}{suffix}"
        write_pyarrow(f"{out}/{name}.orc", values, unit, instant, rng)
        record(report, out, name, f"{out}/{name}.orc", values, unit, instant)
        if unit == "ns":
            values = [None if v is None else v // 1000 for v in values]
            name = f"case_{i:04d}_pyorc_{kind}_truncated{suffix}"
        else:
            name = f"case_{i:04d}_pyorc_{kind}{suffix}"
        write_pyorc(f"{out}/{name}.orc", values, instant, rng)
        record(report, out, name, f"{out}/{name}.orc", values, "us", instant)
    write_edges(out, rng, report)
    print(report)


def edge_values():
    boundary = ORC_EPOCH_NS
    values = []
    for centre in [boundary, boundary - 500_000_000, boundary + 500_000_000]:
        values.extend(centre + k for k in range(-1000, 1001))
    values.extend(range(boundary - 1_000_000_000, boundary + 1_000_000_001, 999_983))
    values.extend(
        [0, 1, 999_999, 1_000_000, 999_999_999, -1_000_000_000, -1_000_001,
         1_000_000_000, 1_000_000_001, ORC_EPOCH_NS - 1_000_000_000]
    )
    return values


def write_edges(out, rng, report):
    values = sorted(set(edge_values()))
    micros = sorted({v // 1000 for v in values if not -1_000_000 < v // 1000 < 0})
    for instant in (False, True):
        suffix = "_instant" if instant else ""
        name = f"edge_orc_epoch_pyarrow{suffix}"
        write_pyarrow(f"{out}/{name}.orc", values, "ns", instant, rng)
        record(report, out, name, f"{out}/{name}.orc", values, "ns", instant)
        name = f"edge_orc_epoch_pyorc{suffix}"
        write_pyorc(f"{out}/{name}.orc", micros, instant, rng)
        record(report, out, name, f"{out}/{name}.orc", micros, "us", instant)


if __name__ == "__main__":
    main()
