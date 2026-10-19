#!/usr/bin/env python3
"""Generate valid ORC files for comparing two builds of the reader.

Usage: python3 scripts/fuzz/gen_corpus.py <output-dir>
Requires: pip install pyorc pyarrow

Writes 100 files with pyorc and pyarrow (ORC C++) covering every column type, every codec the
reader supports, file versions 0.11 and 0.12 (pyarrow only), nulls, several stripes, row indexes
with strides from 1 to 1,000,000 or none, bloom filters, dictionary and direct strings, and
empty files. Compare with `fuzz_mutate hash <files>` from two builds.
"""
import datetime as dt
import decimal
import os
import random
import sys

import pyarrow as pa
import pyarrow.orc as porc
import pyorc

CK = pyorc.CompressionKind
CODECS = {"none": CK.NONE, "zlib": CK.ZLIB, "snappy": CK.SNAPPY, "lz4": CK.LZ4, "zstd": CK.ZSTD}
ARROW_CODECS = {"none": "uncompressed", "zlib": "zlib", "snappy": "snappy", "lz4": "lz4",
                "zstd": "zstd"}
TYPES = [
    "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "string", "varchar(7)",
    "char(4)", "binary", "date", "timestamp", "timestamp with local time zone", "decimal(9,2)",
    "decimal(18,4)", "decimal(30,6)", "decimal(38,10)",
]
NESTED = [
    "array<int>", "array<string>", "map<string,int>", "map<int,array<double>>",
    "struct<a:int,b:string,c:array<bigint>>", "array<struct<x:int,y:map<string,double>>>",
    "uniontype<int,string,double>", "struct<u:uniontype<boolean,bigint>,s:struct<t:string>>",
]
rng = random.Random(20260719)


def scalar(kind, nulls):
    if nulls and rng.random() < 0.2:
        return None
    if kind == "boolean":
        return rng.random() < 0.5
    if kind in ("tinyint", "smallint", "int", "bigint"):
        bits = {"tinyint": 7, "smallint": 15, "int": 31, "bigint": 63}[kind]
        return rng.choice([0, 1, -1, rng.randrange(-(1 << bits), 1 << bits),
                           rng.randrange(0, 40)])
    if kind in ("float", "double"):
        return rng.choice([0.0, rng.uniform(-1e9, 1e9), rng.uniform(-1, 1)])
    if kind == "string":
        return rng.choice(["", "a", "bb", "x" * rng.randrange(0, 300), "é€", "k%d" % rng.randrange(40)])
    if kind.startswith("varchar") or kind.startswith("char"):
        return rng.choice(["ab", "cdef", "g"])
    if kind == "binary":
        return bytes(rng.randrange(256) for _ in range(rng.randrange(0, 20)))
    if kind == "date":
        return dt.date(1900, 1, 1) + dt.timedelta(days=rng.randrange(0, 80000))
    if kind.startswith("timestamp"):
        t = dt.datetime(1950, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(
            seconds=rng.randrange(0, 3 * 10**9), microseconds=rng.randrange(0, 10**6))
        return t if "local" in kind else t.replace(tzinfo=None)
    if kind.startswith("decimal"):
        precision, scale = (int(x) for x in kind[8:-1].split(","))
        return decimal.Decimal(rng.randrange(-(10 ** min(precision, 15)), 10 ** min(precision, 15))
                               ).scaleb(-scale)
    raise ValueError(kind)


def value(td, nulls):
    td = pyorc.TypeDescription.from_string(td) if isinstance(td, str) else td
    name = td.name
    if name == "array":
        return None if nulls and rng.random() < 0.2 else [
            value(td.type, nulls) for _ in range(rng.randrange(0, 5))]
    if name == "map":
        return None if nulls and rng.random() < 0.2 else {
            scalar(str(td.key), False): value(td.value, nulls) for _ in range(rng.randrange(0, 4))}
    if name == "struct":
        return None if nulls and rng.random() < 0.2 else {
            k: value(v, nulls) for k, v in td.fields.items()}
    if name == "uniontype":
        if nulls and rng.random() < 0.2:
            return None
        return value(rng.choice(td.cont_types), nulls)
    return scalar(str(td), nulls)


def row(schema, nulls):
    td = pyorc.TypeDescription.from_string(schema)
    return {k: value(v, nulls) for k, v in td.fields.items()}


def write_pyorc(path, schema, rows, codec, nulls, **kw):
    with open(path, "wb") as f:
        with pyorc.Writer(f, schema, compression=codec, struct_repr=pyorc.StructRepr.DICT,
                          **kw) as w:
            for _ in range(rows):
                w.write(row(schema, nulls))


def arrow_table(rows, nulls):
    ints = [None if nulls and rng.random() < 0.2 else rng.randrange(-10**9, 10**9)
            for _ in range(rows)]
    strs = [None if nulls and rng.random() < 0.2 else "s%d" % rng.randrange(60)
            for _ in range(rows)]
    dbl = [None if nulls and rng.random() < 0.2 else rng.uniform(-1e6, 1e6) for _ in range(rows)]
    stamps = [None if nulls and rng.random() < 0.2 else dt.datetime(2001, 1, 1) + dt.timedelta(
        seconds=rng.randrange(0, 10**8), microseconds=rng.randrange(0, 10**6)) for _ in range(rows)]
    decs = [None if nulls and rng.random() < 0.2 else decimal.Decimal(rng.randrange(-10**8, 10**8)
                                                                     ).scaleb(-3)
            for _ in range(rows)]
    return pa.table({
        "i": pa.array(ints, pa.int64()), "s": pa.array(strs, pa.string()),
        "d": pa.array(dbl, pa.float64()), "t": pa.array(stamps, pa.timestamp("us")),
        "m": pa.array(decs, pa.decimal128(12, 3)),
        "l": pa.array([None if nulls and rng.random() < 0.2 else [rng.randrange(9)] * rng.randrange(4)
                       for _ in range(rows)], pa.list_(pa.int32())),
    })


def main(out):
    os.makedirs(out, exist_ok=True)
    count = 0

    def name(label):
        nonlocal count
        count += 1
        return f"{out}/{count:03d}_{label}.orc"

    strides = [1, 7, 100, 10000, 1_000_000, 0]
    codecs = list(CODECS)
    for i, t in enumerate(TYPES):
        codec = codecs[i % len(codecs)]
        write_pyorc(name(f"scalar_{t.split('(')[0].replace(' ', '_')}_{codec}"),
                    f"struct<c:{t}>", 250, CODECS[codec], True,
                    row_index_stride=strides[i % len(strides)] or 10000,
                    memory_block_size=1024, compression_block_size=1024 * (1 + i % 8))
    for i, t in enumerate(NESTED):
        codec = codecs[(i + 2) % len(codecs)]
        write_pyorc(name(f"nested_{i}_{codec}"), f"struct<c:{t}>", 200, CODECS[codec], True,
                    row_index_stride=strides[i % len(strides)] or 10000)
    for i, t in enumerate(TYPES):
        write_pyorc(name(f"nonnull_{t.split('(')[0].replace(' ', '_')}"), f"struct<c:{t}>", 120,
                    CK.ZLIB if i % 2 else CK.NONE, False, row_index_stride=strides[(i + 1) % 5])
    wide = "struct<a:boolean,b:tinyint,c:smallint,d:int,e:bigint,f:float,g:double,h:string," \
           "i:varchar(7),j:char(4),k:binary,l:date,m:timestamp,n:decimal(18,4),o:decimal(30,6)>"
    for codec in codecs:
        write_pyorc(name(f"wide_{codec}"), wide, 300, CODECS[codec], True, row_index_stride=64)
    for codec in codecs:
        write_pyorc(name(f"stripes_{codec}"), "struct<a:int,b:string,c:double>", 600,
                    CODECS[codec], True, stripe_size=100, batch_size=25, row_index_stride=30)
    for stride in (1, 3, 50, 10_000, 1_000_000):
        write_pyorc(name(f"stride_{stride}"), "struct<a:bigint,s:string>", 400, CK.ZLIB, True,
                    row_index_stride=stride if stride > 1 else 10000)
    for threshold in (0.0, 0.5, 1.0):
        for codec in ("none", "zstd"):
            write_pyorc(name(f"dict_{threshold}_{codec}"), "struct<s:string,v:varchar(7),c:char(4)>",
                        500, CODECS[codec], True, dict_key_size_threshold=threshold,
                        row_index_stride=50)
    for codec in ("none", "snappy", "zlib"):
        write_pyorc(name(f"bloom_{codec}"), "struct<a:int,s:string,d:double,t:timestamp>", 300,
                    CODECS[codec], True, bloom_filter_columns=["a", "s", "d", "t"],
                    row_index_stride=40, bloom_filter_fpp=0.05)
    write_pyorc(name("bloom_stripes"), "struct<a:int,s:string>", 600, CK.NONE, True,
                bloom_filter_columns=["a", "s"], row_index_stride=25, stripe_size=100,
                batch_size=50)
    write_pyorc(name("empty_none"), "struct<a:int,s:string>", 0, CK.NONE, True)
    write_pyorc(name("empty_zlib"), "struct<a:int,m:map<string,int>>", 0, CK.ZLIB, True)
    write_pyorc(name("one_row"), "struct<a:int,s:string,d:decimal(10,2)>", 1, CK.SNAPPY, True)
    write_pyorc(name("all_null"), "struct<a:int,s:string>", 100, CK.ZLIB, True)
    for i, (codec, rows, nulls) in enumerate(
            [(c, r, n) for c in codecs for r in (1, 250, 5000) for n in (False, True)]):
        table = arrow_table(rows, nulls)
        with open(name(f"arrow_{codec}_{rows}_{'n' if nulls else 'f'}"), "wb") as f:
            porc.write_table(table, f, compression=ARROW_CODECS[codec],
                             row_index_stride=[10, 100, 10000][i % 3],
                             stripe_size=[2000, 64 * 1024, 64 * 1024 * 1024][i % 3],
                             bloom_filter_columns=[0, 1] if i % 4 == 0 else None,
                             file_version=["0.11", "0.12"][i % 2],
                             dictionary_key_size_threshold=[0.0, 0.8][i % 2])
    for stripe_size in (1000, 20000):
        for codec in ("none", "zstd"):
            path = name(f"arrow_batches_{stripe_size}_{codec}")
            with open(path, "wb") as f:
                w = porc.ORCWriter(f, compression=ARROW_CODECS[codec], stripe_size=stripe_size,
                                   row_index_stride=500, batch_size=500)
                for _ in range(40):
                    w.write(arrow_table(500, True))
                w.close()
    empty = arrow_table(0, False)
    with open(name("arrow_empty"), "wb") as f:
        porc.write_table(empty, f)
    print(count, "files")


if __name__ == "__main__":
    main(sys.argv[1])
