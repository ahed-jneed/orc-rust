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
Writes ORC files with Apache ORC C++, through pyarrow and through pyorc, for
check_orc_corpus.py: every codec, stripe size, row index stride, compression strategy,
padding tolerance, dictionary threshold and bloom filter, with nulls in every column.

usage: generate_orc_cpp_corpus.py OUT_DIR pyarrow
       generate_orc_cpp_corpus.py OUT_DIR pyorc [INDEX]

pyorc crashes now and then, so run it once for each INDEX from 0 to 59 and repeat the
runs that crash.
"""

import sys, os, random, io, datetime, decimal, itertools
import pyarrow as pa, pyarrow.orc as paorc
import pyorc

out = sys.argv[1]
mode = sys.argv[2]
os.makedirs(out, exist_ok=True)
rng = random.Random("cpp" + mode)

def nullable(vals, p):
    return [None if rng.random() < p else v for v in vals]

def arrow_table(n, p):
    ints = lambda lo, hi: nullable([rng.randint(lo, hi) for _ in range(n)], p)
    cols = {
        "b": pa.array(nullable([rng.random() < .5 for _ in range(n)], p), pa.bool_()),
        "t": pa.array(ints(-128, 127), pa.int8()),
        "s": pa.array(ints(-32768, 32767), pa.int16()),
        "i": pa.array(ints(-2**31, 2**31 - 1), pa.int32()),
        "l": pa.array(ints(-2**63, 2**63 - 1), pa.int64()),
        "f": pa.array(nullable([rng.uniform(-1e6, 1e6) for _ in range(n)], p), pa.float32()),
        "d": pa.array(nullable([rng.uniform(-1e12, 1e12) for _ in range(n)], p), pa.float64()),
        "str": pa.array(nullable([rng.choice(["a", "bb", "", "k%d" % rng.randint(0, 9), "x" * rng.randint(0, 40)]) for _ in range(n)], p)),
        "bin": pa.array(nullable([bytes(rng.randrange(256) for _ in range(rng.randint(0, 9))) for _ in range(n)], p), pa.binary()),
        "dt": pa.array(nullable([datetime.date(1970, 1, 1) + datetime.timedelta(days=rng.randint(-20000, 40000)) for _ in range(n)], p), pa.date32()),
        "ts": pa.array(nullable([datetime.datetime(1990, 1, 1) + datetime.timedelta(seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999)) for _ in range(n)], p), pa.timestamp("ns")),
        "dec": pa.array(nullable([decimal.Decimal(rng.randint(-10**9, 10**9)).scaleb(-2) for _ in range(n)], p), pa.decimal128(10, 2)),
        "dec2": pa.array(nullable([decimal.Decimal(rng.randint(-10**28, 10**28)).scaleb(-5) for _ in range(n)], p), pa.decimal128(30, 5)),
        "st": pa.array(nullable([{"x": rng.randint(0, 9), "y": rng.choice([None, "z"])} for _ in range(n)], p), pa.struct([("x", pa.int32()), ("y", pa.string())])),
        "li": pa.array(nullable([nullable([rng.randint(0, 99) for _ in range(rng.randint(0, 4))], p) for _ in range(n)], p), pa.list_(pa.int32())),
        "m": pa.array(nullable([[("k%d" % j, rng.choice([None, j])) for j in range(rng.randint(0, 3))] for _ in range(n)], p), pa.map_(pa.string(), pa.int32())),
    }
    return pa.table(cols)

CODECS_PA = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]
count = 0
if mode == "pyarrow":
    for k in range(60):
        codec = CODECS_PA[k % 5]
        n = rng.choice([0, 1, 5, 1000, 5000, 30000])
        p = rng.choice([0, 0.2, 0.9, 1.0])
        t = arrow_table(n, p)
        opts = dict(
            file_version=rng.choice(["0.11", "0.12"]),
            batch_size=rng.choice([64, 1024, 4096]),
            stripe_size=rng.choice([1, 100000, 67108864]),
            compression=codec,
            compression_block_size=rng.choice([65536, 131072, 262144]),
            compression_strategy=rng.choice(["speed", "compression"]),
            row_index_stride=rng.choice([100, 1000, 10000]),
            padding_tolerance=rng.choice([0.0, 0.05, 1.0]),
            dictionary_key_size_threshold=rng.choice([0.0, 0.5, 1.0]),
            bloom_filter_columns=rng.choice([None, [3, 8], [4, 12, 11]]),
        )
        try:
            paorc.write_table(t, os.path.join(out, "pa_%02d_%s.orc" % (k, codec)), **opts)
            count += 1
        except Exception as e:
            print("FAIL", k, e)
else:
    SCHEMAS = [
        "struct<a:int,b:string,c:bigint,d:double,e:boolean,f:binary,g:date,h:timestamp,i:decimal(10,2),j:decimal(30,5),k:float,l:tinyint,m:smallint>",
        "struct<a:struct<b:struct<c:int,d:string>,e:array<struct<f:bigint,g:string>>>,m:map<string,struct<x:int>>,k:int>",
        "struct<l1:array<int>,l2:array<array<string>>,m1:map<int,array<double>>,t:array<timestamp>,v:varchar(7),c:char(4)>",
        "struct<u1:uniontype<int,string>,k:int>",
        "struct<u3:uniontype<struct<s:string,a:int,b:string>,array<int>>,k:int>",
    ]
    def gen(td, p):
        K = td.kind
        if rng.random() < p and K != pyorc.TypeKind.STRUCT or (K == pyorc.TypeKind.STRUCT and rng.random() < p / 2):
            return None
        if K == pyorc.TypeKind.STRUCT:
            return tuple(gen(c, p) for c in td.fields.values())
        if K == pyorc.TypeKind.LIST: return [gen(td.type, p) for _ in range(rng.randint(0, 3))]
        if K == pyorc.TypeKind.MAP: return {"k%d" % j: gen(td.value, p) for j in range(rng.randint(0, 3))} if td.key.kind == pyorc.TypeKind.STRING else {j: gen(td.value, p) for j in range(rng.randint(0, 3))}
        if K == pyorc.TypeKind.UNION:
            c = td.cont_types[rng.randrange(len(td.cont_types))]
            v = gen(c, 0)
            return v
        if K == pyorc.TypeKind.BOOLEAN: return rng.random() < .5
        if K == pyorc.TypeKind.BYTE: return rng.randint(-128, 127)
        if K == pyorc.TypeKind.SHORT: return rng.randint(-32768, 32767)
        if K == pyorc.TypeKind.INT: return rng.randint(-2**31, 2**31 - 1)
        if K == pyorc.TypeKind.LONG: return rng.randint(-2**63, 2**63 - 1)
        if K == pyorc.TypeKind.FLOAT: return rng.uniform(-1e5, 1e5)
        if K == pyorc.TypeKind.DOUBLE: return rng.uniform(-1e12, 1e12)
        if K == pyorc.TypeKind.STRING: return rng.choice(["a", "bb", "", "k%d" % rng.randint(0, 9), "x" * rng.randint(0, 40)])
        if K == pyorc.TypeKind.VARCHAR: return "v%d" % rng.randint(0, 99)
        if K == pyorc.TypeKind.CHAR: return "c%d  " % rng.randint(0, 9)
        if K == pyorc.TypeKind.BINARY: return bytes(rng.randrange(256) for _ in range(rng.randint(0, 9)))
        if K == pyorc.TypeKind.DATE: return datetime.date(1970, 1, 1) + datetime.timedelta(days=rng.randint(-20000, 40000))
        if K == pyorc.TypeKind.TIMESTAMP: return datetime.datetime(1990, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999))
        if K == pyorc.TypeKind.DECIMAL: return decimal.Decimal(rng.randint(-10**(td.precision - 1), 10**(td.precision - 1))).scaleb(-td.scale)
        raise AssertionError(K)
    codecs = [pyorc.CompressionKind.NONE, pyorc.CompressionKind.ZLIB, pyorc.CompressionKind.SNAPPY, pyorc.CompressionKind.LZ4, pyorc.CompressionKind.ZSTD]
    for k in ([int(sys.argv[3])] if len(sys.argv) > 3 else range(60)):
        rng.seed("cpp%s%d" % (mode, k))
        schema = SCHEMAS[k % len(SCHEMAS)]
        union = "uniontype" in schema
        td = pyorc.TypeDescription.from_string(schema)
        n = rng.choice([0, 1, 5, 300, 900]) if union else rng.choice([0, 1, 5, 1000, 5000])
        p = rng.choice([0, 0.2, 0.9, 1.0])
        codec = codecs[k % len(codecs)]
        buf = io.BytesIO()
        try:
            w = pyorc.Writer(buf, schema, batch_size=1024 if union else rng.choice([64, 1024]), stripe_size=rng.choice([1, 100000, 67108864]) if not union else 67108864,
                             row_index_stride=rng.choice([0, 100, 1000, 10000]) if not union else 10000, compression=codec,
                             compression_strategy=rng.choice([pyorc.CompressionStrategy.SPEED, pyorc.CompressionStrategy.COMPRESSION]),
                             compression_block_size=rng.choice([65536, 131072]),
                             padding_tolerance=rng.choice([0.0, 0.05]), dict_key_size_threshold=rng.choice([0.0, 0.5, 1.0]),
                             bloom_filter_columns=rng.choice([None, ["a"]]) if schema.startswith("struct<a:int") else None)
            for _ in range(n):
                row = None if rng.random() < p / 3 else gen(td, p)
                w.write(row)
            w.close()
        except Exception as e:
            print("FAIL", k, schema[:30], e)
            continue
        open(os.path.join(out, "po_%02d_%s_s%d.orc" % (k, codec.name.lower(), k % len(SCHEMAS))), "wb").write(buf.getvalue())
        count += 1
print("written", count)
