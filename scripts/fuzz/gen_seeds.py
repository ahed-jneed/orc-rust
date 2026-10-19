#!/usr/bin/env python3
"""Generate small seed ORC files covering every column type and codec.

Usage: python3 scripts/fuzz/gen_seeds.py <output-dir>
Requires: pip install pyorc
"""
import datetime as dt
import decimal
import os
import random
import sys

import pyorc

CK = pyorc.CompressionKind
CODECS = {"none": CK.NONE, "zlib": CK.ZLIB, "snappy": CK.SNAPPY, "lz4": CK.LZ4, "zstd": CK.ZSTD}
N = 300
rng = random.Random(7)


def maybe_null(v, nulls):
    return None if nulls and rng.random() < 0.25 else v


def gen(kind, nulls):
    if kind == "boolean":
        return maybe_null(rng.random() < 0.5, nulls)
    if kind in ("tinyint", "smallint", "int", "bigint"):
        bits = {"tinyint": 7, "smallint": 15, "int": 31, "bigint": 62}[kind]
        mode = rng.random()
        if mode < 0.3:
            v = 5
        elif mode < 0.6:
            v = rng.randrange(0, 50)
        else:
            v = rng.randrange(-(1 << bits), 1 << bits)
        return maybe_null(v, nulls)
    if kind in ("float", "double"):
        return maybe_null(rng.uniform(-1e6, 1e6), nulls)
    if kind == "string":
        return maybe_null(rng.choice(["a", "bb", "ccc", "dddd"]) + str(rng.randrange(0, 20)), nulls)
    if kind.startswith("varchar") or kind.startswith("char"):
        return maybe_null(rng.choice(["ab", "cde", "fghij"]), nulls)
    if kind == "binary":
        return maybe_null(bytes(rng.randrange(256) for _ in range(rng.randrange(0, 12))), nulls)
    if kind == "date":
        return maybe_null(dt.date(1990, 1, 1) + dt.timedelta(days=rng.randrange(0, 20000)), nulls)
    if kind.startswith("timestamp"):
        t = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(
            seconds=rng.randrange(0, 10**9), microseconds=rng.randrange(0, 10**6)
        )
        return maybe_null(t if "local" in kind else t.replace(tzinfo=None), nulls)
    if kind.startswith("decimal"):
        return maybe_null(decimal.Decimal(rng.randrange(-10**9, 10**9)) / 100, nulls)
    raise ValueError(kind)


def value(td, nulls):
    if isinstance(td, str):
        td = pyorc.TypeDescription.from_string(td)
    name = td.name
    if name == "array":
        return maybe_null([value(td.type, nulls) for _ in range(rng.randrange(0, 4))], nulls)
    if name == "map":
        return maybe_null(
            {gen(str(td.key), False): value(td.value, nulls) for _ in range(rng.randrange(0, 4))}, nulls
        )
    if name == "struct":
        return maybe_null({k: value(v, nulls) for k, v in td.fields.items()}, nulls)
    if name == "uniontype":
        return union_row(nulls)
    return gen(str(td), nulls)


def union_row(nulls):
    if nulls and rng.random() < 0.2:
        return None
    return rng.choice([rng.randrange(100), "u%d" % rng.randrange(10)])


def write(path, schema, codec, nulls=False, rows=N, **kw):
    rows = [value(schema, nulls) for _ in range(rows)]
    with open(path, "wb") as f:
        with pyorc.Writer(f, schema, compression=codec, struct_repr=pyorc.StructRepr.DICT, **kw) as w:
            for r in rows:
                w.write(r)


def main(out):
    os.makedirs(out, exist_ok=True)
    scalars = [
        "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "string",
        "varchar(5)", "char(5)", "binary", "date", "timestamp", "timestamp with local time zone",
        "decimal(10,2)", "decimal(30,2)",
    ]
    for t in scalars:
        name = t.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "_")
        write(f"{out}/scalar_{name}_none.orc", f"struct<c:{t}>", CK.NONE, nulls=True,
              row_index_stride=50)
    for cname, codec in CODECS.items():
        write(f"{out}/mixed_{cname}.orc",
              "struct<a:int,b:string,c:double,d:bigint,e:boolean,f:timestamp>", codec, nulls=True,
              row_index_stride=64, compression_block_size=512, memory_block_size=256)
    write(f"{out}/string_dict.orc", "struct<s:string>", CK.ZLIB, dict_key_size_threshold=1.0,
          row_index_stride=50)
    write(f"{out}/string_direct.orc", "struct<s:string>", CK.SNAPPY, dict_key_size_threshold=0.0,
          row_index_stride=50, nulls=True)
    write(f"{out}/bloom_none.orc", "struct<a:int,s:string,d:double>", CK.NONE,
          bloom_filter_columns=[0, 1, 2], row_index_stride=50)
    write(f"{out}/bloom_zstd.orc", "struct<a:bigint,s:string>", CK.ZSTD,
          bloom_filter_columns=["a", "s"], row_index_stride=40, nulls=True)
    write(f"{out}/list_int.orc", "struct<l:array<int>>", CK.ZLIB, nulls=True, row_index_stride=50)
    write(f"{out}/map_str_int.orc", "struct<m:map<string,int>>", CK.LZ4, nulls=True,
          row_index_stride=50, compression_block_size=1024, memory_block_size=256)
    write(f"{out}/struct_nested.orc", "struct<s:struct<a:int,b:string>>", CK.NONE, nulls=True)
    write(f"{out}/union.orc", "struct<u:uniontype<int,string>>", CK.NONE, nulls=True)
    write(f"{out}/nested.orc",
          "struct<l:array<struct<a:int,b:array<string>>>,m:map<int,array<double>>>",
          CK.ZSTD, nulls=True, row_index_stride=50)
    write(f"{out}/multi_stripe_zlib.orc", "struct<a:int,b:string>", CK.ZLIB, nulls=True,
          stripe_size=100, row_index_stride=50, compression_block_size=512, memory_block_size=256,
          batch_size=25)
    write(f"{out}/multi_stripe_none.orc", "struct<a:bigint,b:string>",
          CK.NONE, stripe_size=1024, row_index_stride=50, dict_key_size_threshold=0.0,
          batch_size=50)
    write(f"{out}/multi_stripe_mixed_none.orc",
          "struct<a:int,b:string,c:double,d:bigint,e:boolean,f:timestamp,g:decimal(12,3)>",
          CK.NONE, nulls=True, stripe_size=100, row_index_stride=40, batch_size=25)
    write(f"{out}/multi_stripe_nested_none.orc",
          "struct<l:array<struct<a:int,b:array<string>>>,m:map<int,array<double>>>",
          CK.NONE, nulls=True, stripe_size=100, row_index_stride=40, batch_size=25)
    write(f"{out}/multi_stripe_dict_lz4.orc", "struct<s:string,t:varchar(5),d:date>", CK.LZ4,
          nulls=True, stripe_size=100, row_index_stride=30, batch_size=25,
          dict_key_size_threshold=1.0)
    write(f"{out}/multi_stripe_bloom_none.orc", "struct<a:int,s:string>", CK.NONE, nulls=True,
          stripe_size=1000, row_index_stride=25, batch_size=100, bloom_filter_columns=["a", "s"])
    write(f"{out}/empty.orc", "struct<a:int,s:string>", CK.NONE, rows=0)


if __name__ == "__main__":
    main(sys.argv[1])
