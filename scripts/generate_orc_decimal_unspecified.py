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
Writes tests/basic/data/pyorc_decimal_unspecified_{lossy,exact,overflow}.orc with
pyorc and rewrites the footer so that the decimal columns record no precision and
no scale, like the files Hive 0.11 wrote. The values of column sN keep the scale N
in the SECONDARY stream. Column rec12 records a scale of 6 and no precision.

usage: generate_orc_decimal_unspecified.py OUT_DIR
"""

import sys
from decimal import Decimal, getcontext

import pyarrow.orc as porc
import pyorc

getcontext().prec = 80

SCALES = [0, 3, 6, 7, 10, 11, 18, 19, 38]
RECORDED_SCALE = 6


def varint(n):
    out = bytearray()
    while True:
        low = n & 0x7F
        n >>= 7
        if n:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def read_varint(data, i):
    n = 0
    shift = 0
    while True:
        b = data[i]
        i += 1
        n |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return n, i


def type_entry(precision, scale):
    return bytes([0x22, 0x08, 0x08, 0x0E, 0x20, 0x00, 0x28, precision, 0x30, scale])


def rewrite_footer(data, replacements):
    ps_len = data[-1]
    ps = data[-1 - ps_len : -1]
    assert ps[0] == 0x08
    footer_len, after = read_varint(ps, 1)
    start = len(data) - 1 - ps_len - footer_len
    footer = data[start : len(data) - 1 - ps_len]
    for old, new in replacements:
        assert footer.count(old) == 1, old
        footer = footer.replace(old, new)
    ps = b"\x08" + varint(len(footer)) + ps[after:]
    assert len(ps) == ps_len
    return data[:start] + footer + ps + bytes([len(ps)])


def write(path, columns, rows, recorded):
    schema = "struct<%s>" % ",".join(f"{n}:decimal(38,{s})" for n, s in columns)
    with open(path, "wb") as f:
        with pyorc.Writer(
            f, schema, compression=pyorc.CompressionKind.NONE
        ) as w:
            for row in rows:
                w.write(tuple(row))
    with open(path, "rb") as f:
        data = f.read()
    replacements = []
    for name, s in columns:
        if name in recorded:
            new = bytes([0x22, 0x04, 0x08, 0x0E, 0x30, recorded[name]])
        else:
            new = bytes([0x22, 0x02, 0x08, 0x0E])
        replacements.append((type_entry(38, s), new))
    with open(path, "wb") as f:
        f.write(rewrite_footer(data, replacements))


def unit(s):
    return Decimal(1).scaleb(-s)


def lossy_column(s):
    q = unit(s)
    integer = Decimal(0) if s == 38 else Decimal(1)
    return [Decimal(0), q, -q, integer + 5 * q, 12345 * q, -98765 * q]


def exact_column(s):
    q = unit(s)
    if s == 0:
        return [Decimal(0), Decimal(1), Decimal(-3), Decimal(2)]
    if s == 38:
        return [Decimal(0), Decimal("0.5"), Decimal("-0.5"), Decimal("0.25")]
    return [
        Decimal(0),
        Decimal("1.2").quantize(q),
        Decimal("-3.5").quantize(q),
        Decimal("0.25").quantize(q),
    ]


def transpose(columns):
    return list(zip(*columns))


def main(out):
    lossy = [(f"s{s}", s) for s in SCALES] + [("rec12", 12)]
    lossy_data = [lossy_column(s) for s in SCALES] + [
        [Decimal(0), Decimal("1.234567891234"), Decimal("-0.000001234567"), Decimal("5.5")]
        + [Decimal(0)] * 2
    ]
    path = f"{out}/pyorc_decimal_unspecified_lossy.orc"
    write(path, lossy, transpose(lossy_data), {"rec12": RECORDED_SCALE})

    exact = [(f"s{s}", s) for s in SCALES]
    exact_data = [exact_column(s) for s in SCALES]
    path_exact = f"{out}/pyorc_decimal_unspecified_exact.orc"
    write(path_exact, exact, transpose(exact_data), {})

    overflow = [("ov38", 0), ("ov29", 1)]
    overflow_data = [
        [Decimal("9" * 38), Decimal(1)],
        [Decimal(10) ** 28, Decimal(1)],
    ]
    path_overflow = f"{out}/pyorc_decimal_unspecified_overflow.orc"
    write(path_overflow, overflow, transpose(overflow_data), {})

    table = porc.read_table(path)
    for name, s in lossy:
        want = lossy_data[[n for n, _ in lossy].index(name)]
        got = table.column(name).to_pylist()
        assert table.schema.field(name).type.scale == 6, name
        for g, w in zip(got, want):
            assert g == w.quantize(unit(6), rounding="ROUND_DOWN"), (name, g, w)
    table = porc.read_table(path_exact)
    for name, s in exact:
        want = exact_data[[n for n, _ in exact].index(name)]
        for g, w in zip(table.column(name).to_pylist(), want):
            assert g == w.quantize(unit(6), rounding="ROUND_DOWN"), (name, g, w)


if __name__ == "__main__":
    main(sys.argv[1])
