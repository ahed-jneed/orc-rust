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
Writes ORC files with pyorc (Apache ORC C++) whose rows are null at the root, in
nested structs, lists, maps and unions, in every primitive type, together with the
rows read back, to be checked by tests/basic/null_matrix.rs.

usage: generate_orc_null_matrix.py OUT_DIR

Every case is OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected. The first line of the
expected file holds the column names separated by tabs, every other line is one row
with one rendered cell per column, separated by tabs. A cell is "~" for null (a row
that is null at the root renders "~" in every column), the decimal integer for
integers, dates (days), timestamps (nanoseconds since the epoch) and decimals (the
unscaled value), "T" or "F" for a boolean, the bits in hex for a float or a double,
the bytes in hex for a binary, the string itself, "{a=X,b=Y}" for a struct, "[X,Y]"
for a list, "{K=V,K=V}" for a map and "<TAG:X>" for a union value.

The expected cells come from pyarrow; the same rows rendered from the values pyorc
reads back must be identical, which the script asserts.
"""

import datetime
import decimal
import io
import os
import random
import struct
import subprocess
import sys

import pyarrow as pa
import pyarrow.orc as paorc
import pyorc

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
KIND = pyorc.TypeKind
DECIMALS = decimal.Context(prec=60)


def float_bits(v, fmt, width):
    (bits,) = struct.unpack("<" + fmt, struct.pack("<" + ("f" if width == 8 else "d"), v))
    return format(bits, "0%dx" % width)


def render_scalar(s):
    t = s.type
    if not s.is_valid:
        return "~"
    if pa.types.is_union(t):
        inner = s.value
        if not inner.is_valid:
            return "~"
        return "<%d:%s>" % (s.type_code, render_scalar(inner))
    if pa.types.is_struct(t):
        return "{" + ",".join("%s=%s" % (f.name, render_scalar(s[f.name])) for f in t) + "}"
    if pa.types.is_map(t):
        return "{" + ",".join(
            "%s=%s" % (render_scalar(kv["key"]), render_scalar(kv["value"])) for kv in s.values
        ) + "}"
    if pa.types.is_list(t):
        return "[" + ",".join(render_scalar(v) for v in s.values) + "]"
    if pa.types.is_boolean(t):
        return "T" if s.as_py() else "F"
    if pa.types.is_float32(t):
        return float_bits(s.as_py(), "I", 8)
    if pa.types.is_float64(t):
        return float_bits(s.as_py(), "Q", 16)
    if pa.types.is_decimal(t):
        return str(int(s.as_py().scaleb(t.scale, DECIMALS)))
    if pa.types.is_timestamp(t) or pa.types.is_date32(t):
        return str(s.value)
    if pa.types.is_binary(t):
        return s.as_py().hex()
    return str(s.as_py())


def variant_of(value, variants):
    for index, variant in enumerate(variants):
        kind = variant.kind
        if kind in (KIND.INT, KIND.LONG, KIND.SHORT, KIND.BYTE) and isinstance(value, int):
            return index
        if kind == KIND.STRING and isinstance(value, str):
            return index
        if kind == KIND.STRUCT and isinstance(value, tuple):
            return index
        if kind == KIND.LIST and isinstance(value, list):
            return index
    raise AssertionError((value, variants))


def render_value(v, td):
    if v is None:
        return "~"
    kind = td.kind
    if kind == KIND.STRUCT:
        return "{" + ",".join(
            "%s=%s" % (name, render_value(x, td.fields[name])) for name, x in zip(td.fields, v)
        ) + "}"
    if kind == KIND.MAP:
        key_td, value_td = td.key, td.value
        return "{" + ",".join(
            "%s=%s" % (render_value(k, key_td), render_value(x, value_td)) for k, x in v.items()
        ) + "}"
    if kind == KIND.LIST:
        return "[" + ",".join(render_value(x, td.type) for x in v) + "]"
    if kind == KIND.UNION:
        index = variant_of(v, td.cont_types)
        return "<%d:%s>" % (index, render_value(v, td.cont_types[index]))
    if kind == KIND.BOOLEAN:
        return "T" if v else "F"
    if kind == KIND.FLOAT:
        return float_bits(v, "I", 8)
    if kind == KIND.DOUBLE:
        return float_bits(v, "Q", 16)
    if kind == KIND.DECIMAL:
        return str(int(v.scaleb(td.scale, DECIMALS)))
    if kind == KIND.TIMESTAMP:
        d = v - EPOCH
        return str((d.days * 86400 + d.seconds) * 10**9 + d.microseconds * 1000)
    if kind == KIND.DATE:
        return str((v - datetime.date(1970, 1, 1)).days)
    if kind == KIND.BINARY:
        return v.hex()
    return str(v)


class Blocks:
    """Null patterns that change every few dozen rows so runs, repeats and
    alternations all occur."""

    KINDS = ["none", "all", "alt", "alt2", "rand", "heavy", "light"]

    def __init__(self, rng, kinds=None):
        self.rng = rng
        self.kinds = kinds or self.KINDS
        self.left = 0
        self.kind = "none"
        self.i = 0

    def null(self):
        if self.left == 0:
            self.kind = self.rng.choice(self.kinds)
            self.left = self.rng.randint(8, 70)
            self.i = 0
        self.left -= 1
        self.i += 1
        k = self.kind
        if k == "none":
            return False
        if k == "all":
            return True
        if k == "alt":
            return self.i % 2 == 0
        if k == "alt2":
            return self.i % 3 != 0
        if k == "rand":
            return self.rng.random() < 0.5
        if k == "heavy":
            return self.rng.random() < 0.9
        return self.rng.random() < 0.1


class Ints:
    """Integers in runs, arithmetic sequences, small ranges and random values."""

    def __init__(self, rng, lo, hi):
        self.rng, self.lo, self.hi = rng, lo, hi
        self.left = 0
        self.kind = "const"
        self.cur = 0
        self.step = 1

    def next(self):
        rng = self.rng
        if self.left == 0:
            self.kind = rng.choice(["const", "seq", "small", "rand", "edge"])
            self.left = rng.randint(3, 40)
            self.cur = rng.randint(self.lo, self.hi)
            self.step = rng.choice([1, 2, -1, 7])
        self.left -= 1
        if self.kind == "const":
            return self.cur
        if self.kind == "seq":
            self.cur = min(max(self.cur + self.step, self.lo), self.hi)
            return self.cur
        if self.kind == "small":
            return min(max(rng.randint(0, 5), self.lo), self.hi)
        if self.kind == "edge":
            return rng.choice([self.lo, self.hi, 0 if self.lo <= 0 <= self.hi else self.lo])
        return rng.randint(self.lo, self.hi)


def value_maker(rng, type_name):
    ints = {
        "tinyint": (-128, 127),
        "smallint": (-32768, 32767),
        "int": (-(2**31), 2**31 - 1),
        "bigint": (-(2**63), 2**63 - 1),
    }
    if type_name in ints:
        gen = Ints(rng, *ints[type_name])
        return gen.next
    if type_name == "boolean":
        gen = Blocks(rng)
        return gen.null
    if type_name == "float":
        return lambda: struct.unpack("<f", struct.pack("<f", rng.uniform(-1e6, 1e6)))[0]
    if type_name == "double":
        return lambda: rng.uniform(-1e12, 1e12)
    if type_name == "string_dict":
        words = ["alpha", "beta", "", "gamma", "delta", "x" * 40]
        return lambda: rng.choice(words)
    if type_name == "string_direct":
        return lambda: "".join(rng.choice("abcdefghij") for _ in range(rng.randint(0, 24)))
    if type_name == "binary":
        return lambda: bytes(rng.randrange(256) for _ in range(rng.randint(0, 12)))
    if type_name == "date":
        gen = Ints(rng, -30000, 60000)
        return lambda: datetime.date(1970, 1, 1) + datetime.timedelta(days=gen.next())
    if type_name == "timestamp":
        gen = Ints(rng, -2 * 10**9, 4 * 10**9)
        return lambda: EPOCH + datetime.timedelta(
            seconds=gen.next(), microseconds=rng.choice([0, 1, 500000, 999999])
        )
    if type_name.startswith("decimal_"):
        _, precision, scale = type_name.split("_")
        precision, scale = int(precision), int(scale)
        gen = Ints(rng, -(10**precision) + 1, 10**precision - 1)
        return lambda: decimal.Decimal(gen.next()).scaleb(-scale, DECIMALS)
    raise AssertionError(type_name)


def orc_type(type_name):
    if type_name.startswith("string_"):
        return "string"
    if type_name.startswith("decimal_"):
        _, precision, scale = type_name.split("_")
        return "decimal(%s,%s)" % (precision, scale)
    return type_name


PRIMITIVES = [
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "string_dict",
    "string_direct",
    "binary",
    "date",
    "timestamp",
    "decimal_10_2",
    "decimal_30_5",
]


def pyarrow_lines(path):
    """The rows pyarrow reads, or None when pyarrow crashes or fails on the file."""
    out = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--render", path],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=60,
    )
    if out.returncode != 0:
        return None
    return out.stdout.split("\n")[:-1]


def render_file(path):
    table = paorc.read_table(path)
    for i in range(table.num_rows):
        print("\t".join(render_scalar(table.column(n)[i]) for n in table.column_names))


def write_case(out_dir, name, schema, rows, **options):
    options.setdefault("compression", pyorc.CompressionKind.ZLIB)
    options.setdefault("batch_size", 1024 if "uniontype" in schema else 64)
    buf = io.BytesIO()
    writer = pyorc.Writer(buf, schema, **options)
    for row in rows:
        writer.write(row)
    writer.close()
    data = buf.getvalue()
    assert len(data) < 20000, (name, len(data))
    path = os.path.join(out_dir, name + ".orc")
    with open(path, "wb") as f:
        f.write(data)
    reader = pyorc.Reader(io.BytesIO(data))
    stripes = reader.num_of_stripes
    td = reader.schema
    names = list(td.fields)
    lines = pyarrow_lines(path)
    py_rows = list(pyorc.Reader(io.BytesIO(data)))
    assert py_rows == rows, name
    py_lines = []
    for row in py_rows:
        if row is None:
            py_lines.append("\t".join("~" for _ in names))
        else:
            py_lines.append("\t".join(render_value(v, td.fields[n]) for n, v in zip(names, row)))
    assert len(py_lines) == len(rows), (name, len(py_lines), len(rows))
    if lines is None:
        lines = py_lines
        print("%s: pyarrow cannot read the file, the expected rows come from pyorc" % name)
    assert lines == py_lines, (name, [(a, b) for a, b in zip(lines, py_lines) if a != b][:3])
    with open(os.path.join(out_dir, name + ".expected"), "w") as f:
        f.write("\t".join(names) + "\n")
        for line in lines:
            f.write(line + "\n")
    return stripes, len(data)


def nullable(rng, blocks, make):
    return None if blocks.null() else make()


def primitive_cases(out_dir, report):
    for type_name in PRIMITIVES:
        rng = random.Random("prim_" + type_name)
        make = value_maker(rng, type_name)
        root, child, other = Blocks(rng), Blocks(rng), Blocks(rng)
        ids = Ints(rng, -1000, 1000)
        rows = []
        for _ in range(400):
            if root.null():
                rows.append(None)
                continue
            rows.append(
                (
                    None if other.null() else ids.next(),
                    nullable(rng, child, make),
                    None if other.null() else rng.choice(["p", "qq", "rrr"]),
                )
            )
        schema = "struct<a:int,c:%s,z:string>" % orc_type(type_name)
        options = {"row_index_stride": 100}
        if type_name == "string_direct":
            options["dict_key_size_threshold"] = 0.0
        if type_name == "string_dict":
            options["dict_key_size_threshold"] = 1.0
        report["prim_" + type_name] = write_case(out_dir, "prim_" + type_name, schema, rows, **options)


def reported_cases(out_dir, report):
    rows = [(None,), None, (None,), (40,), (1763316495,), (-308973329,), (23,), (28,), None]
    report["root_null_int"] = write_case(out_dir, "root_null_int", "struct<c:int>", rows)
    rows = [(None,), None, None, None]
    report["root_null_only"] = write_case(out_dir, "root_null_only", "struct<c:int>", rows)
    rows = [None] * 5
    report["root_null_all"] = write_case(out_dir, "root_null_all", "struct<c:int,d:string>", rows)


def nested_struct_case(out_dir, report):
    rng = random.Random("nested")
    root, s, t, l, leaf = (Blocks(rng) for _ in range(5))
    ints = Ints(rng, -500, 500)
    rows = []
    for _ in range(500):
        if root.null():
            rows.append(None)
            continue
        if s.null():
            inner = None
        else:
            if t.null():
                tv = None
            else:
                items = None if l.null() else [None if leaf.null() else ints.next() for _ in range(rng.randint(0, 3))]
                tv = (None if leaf.null() else rng.choice(["u", "vv", ""]), items)
            inner = (None if leaf.null() else ints.next(), tv, None if leaf.null() else rng.randint(-5, 5) * 10**12)
        rows.append((inner, None if leaf.null() else ints.next()))
    report["nested_struct"] = write_case(
        out_dir,
        "nested_struct",
        "struct<s:struct<a:int,t:struct<b:string,l:array<int>>,u:bigint>,k:int>",
        rows,
        row_index_stride=100,
    )


def list_of_struct_case(out_dir, report):
    rng = random.Random("lists")
    root, lst, elem, leaf = (Blocks(rng) for _ in range(4))
    ints = Ints(rng, -500, 500)
    rows = []
    for _ in range(500):
        if root.null():
            rows.append(None)
            continue
        if lst.null():
            items = None
        else:
            items = []
            for _ in range(rng.choice([0, 1, 2, 3, 5])):
                if elem.null():
                    items.append(None)
                else:
                    items.append(
                        (
                            None if leaf.null() else ints.next(),
                            None if leaf.null() else rng.choice(["a", "bb", "ccc"]),
                        )
                    )
        rows.append((items, None if leaf.null() else ints.next()))
    report["list_of_struct"] = write_case(
        out_dir, "list_of_struct", "struct<l:array<struct<a:int,b:string>>,k:int>", rows, row_index_stride=100
    )


def map_case(out_dir, report):
    rng = random.Random("maps")
    root, mp, val, leaf = (Blocks(rng) for _ in range(4))
    ints = Ints(rng, -500, 500)
    rows = []
    for _ in range(500):
        if root.null():
            rows.append(None)
            continue
        if mp.null():
            m = None
        else:
            m = {}
            for key in rng.sample("abcdefg", rng.choice([0, 1, 2, 4])):
                m[key] = None if val.null() else ints.next()
        if leaf.null():
            n = None
        else:
            n = {}
            for key in rng.sample(range(100), rng.choice([0, 1, 3])):
                n[key] = None if val.null() else (None if leaf.null() else rng.choice(["x", "yy"]),)
        rows.append((m, n, None if leaf.null() else ints.next()))
    report["map_nulls"] = write_case(
        out_dir,
        "map_nulls",
        "struct<m:map<string,int>,n:map<int,struct<x:string>>,k:int>",
        rows,
        row_index_stride=100,
    )


def union_cases(out_dir, report):
    rng = random.Random("unions")
    root, un, leaf = (Blocks(rng) for _ in range(3))
    ints = Ints(rng, -(2**31), 2**31 - 1)
    rows = []
    for _ in range(600):
        if root.null():
            rows.append(None)
        elif un.null():
            rows.append((None, None if leaf.null() else ints.next()))
        else:
            v = ints.next() if rng.random() < 0.5 else rng.choice(["", "a", "bcd"])
            rows.append((v, None if leaf.null() else ints.next()))
    report["union_int_string"] = write_case(
        out_dir, "union_int_string", "struct<u:uniontype<int,string>,k:int>", rows, row_index_stride=100
    )
    rows = [(1,), (5,), ("x",), (None,), (7,), ("yy",), None, (3,), ("",), None]
    report["union_int_string_small"] = write_case(
        out_dir, "union_int_string_small", "struct<u:uniontype<int,string>>", rows
    )
    report["union_no_nulls"] = write_case(out_dir, "union_no_nulls", "struct<u:uniontype<int,string>>", [(i,) for i in range(40)])

    un, leaf, inner = (Blocks(rng) for _ in range(3))
    rows = []
    for _ in range(500):
        if un.null():
            rows.append((None, None if leaf.null() else ints.next()))
        elif rng.random() < 0.5:
            rows.append(
                (
                    (
                        rng.choice(["s", "tt"]),
                        None if inner.null() else ints.next(),
                        None if inner.null() else rng.choice(["s", "tt"]),
                    ),
                    None if leaf.null() else ints.next(),
                )
            )
        else:
            rows.append(
                (
                    [ints.next() for _ in range(rng.randint(0, 3))],
                    None if leaf.null() else ints.next(),
                )
            )
    report["union_struct_list"] = write_case(
        out_dir,
        "union_struct_list",
        "struct<u:uniontype<struct<s:string,a:int,b:string>,array<int>>,k:int>",
        rows,
    )

    root, un, st, leaf = (Blocks(rng) for _ in range(4))
    rows = []

    def union_value():
        if un.null():
            return None
        return ints.next() if rng.random() < 0.5 else rng.choice(["m", "nn"])

    for _ in range(500):
        if root.null():
            rows.append(None)
            continue
        rows.append((None if st.null() else (union_value(),), None if leaf.null() else ints.next()))
    report["union_nested"] = write_case(
        out_dir,
        "union_nested",
        "struct<s:struct<u:uniontype<int,string>>,k:int>",
        rows,
        row_index_stride=100,
    )

    leaf, lst = Blocks(rng), Blocks(rng)
    rows = []
    for _ in range(300):
        items = [ints.next() if rng.random() < 0.5 else rng.choice(["m", "nn"]) for _ in range(rng.randint(0, 3))]
        rows.append((None if lst.null() else items, None if leaf.null() else ints.next()))
    report["union_in_list"] = write_case(
        out_dir, "union_in_list", "struct<l:array<uniontype<int,string>>,k:int>", rows
    )


def multistripe_case(out_dir, report):
    rng = random.Random("stripes")
    ints = Ints(rng, -1000, 1000)
    rows = []
    patterns = [
        (0.0, 0.0),
        (0.5, 0.0),
        (0.0, 0.5),
        (0.9, 0.9),
        (1.0, 0.0),
        (0.0, 1.0),
        (0.2, 0.2),
        (0.0, 0.0),
    ]
    for stripe, (p_root, p_child) in enumerate(patterns):
        for _ in range(150):
            if rng.random() < p_root:
                rows.append(None)
                continue
            rows.append(
                (
                    None if rng.random() < p_child else ints.next(),
                    None if rng.random() < p_child else rng.choice(["k1", "k2", "k3", "longer string"]),
                    None if rng.random() < p_child else [ints.next() for _ in range(rng.randint(0, 2))],
                )
            )
    stripes, size = write_case(
        out_dir,
        "multistripe",
        "struct<a:int,s:string,l:array<int>>",
        rows,
        compression=pyorc.CompressionKind.NONE,
        stripe_size=1,
        batch_size=150,
        row_index_stride=50,
    )
    assert stripes >= 4, stripes
    report["multistripe"] = (stripes, size)


CASES = {
    "reported": reported_cases,
    "primitives": primitive_cases,
    "nested_struct": nested_struct_case,
    "list_of_struct": list_of_struct_case,
    "map": map_case,
    "union": union_cases,
    "multistripe": multistripe_case,
}


def run_case(out_dir, case):
    report = {}
    CASES[case](out_dir, report)
    for name, (stripes, size) in report.items():
        print("%-26s stripes=%d bytes=%d" % (name, stripes, size))


def main(out_dir):
    """pyorc corrupts its heap often enough that every group of cases is written by a
    process of its own, which is retried when it crashes."""
    os.makedirs(out_dir, exist_ok=True)
    for case in CASES:
        for attempt in range(3):
            out = subprocess.run(
                [sys.executable, os.path.abspath(__file__), "--case", case, out_dir],
                stdin=subprocess.DEVNULL,
                timeout=300,
            )
            if out.returncode == 0:
                break
        else:
            raise SystemExit("cannot write the %s cases" % case)


if __name__ == "__main__":
    if sys.argv[1] == "--render":
        render_file(sys.argv[2])
    elif sys.argv[1] == "--case":
        run_case(sys.argv[3], sys.argv[2])
    else:
        main(sys.argv[1])
