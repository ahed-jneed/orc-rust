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
Writes ORC files with Apache Spark (pyspark) for check_orc_corpus.py: every codec, with
nulls in every column, nested types, decimals and timestamps, in one or two part files.

usage: generate_orc_spark_corpus.py OUT_DIR SPARK_VERSION
"""

import sys, os, random, datetime, decimal
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
out = sys.argv[1]; ver = sys.argv[2]
spark = (SparkSession.builder.master("local[2]").appName("orcgen").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").config("spark.driver.host", "127.0.0.1").getOrCreate())
rng = random.Random("spark" + ver)
def n(p, f):
    return None if rng.random() < p else f()
schema = StructType([
    StructField("b", BooleanType()), StructField("t", ByteType()), StructField("s", ShortType()), StructField("i", IntegerType()),
    StructField("l", LongType()), StructField("f", FloatType()), StructField("d", DoubleType()), StructField("str", StringType()),
    StructField("v", StringType()), StructField("bin", BinaryType()), StructField("dt", DateType()), StructField("ts", TimestampType()),
    StructField("tsn", TimestampNTZType()) if hasattr(sys.modules[__name__], "TimestampNTZType") else StructField("tsn", TimestampType()),
    StructField("dec", DecimalType(10, 2)), StructField("dec2", DecimalType(30, 5)), StructField("dec3", DecimalType(38, 18)),
    StructField("st", StructType([StructField("x", IntegerType()), StructField("y", StringType())])),
    StructField("li", ArrayType(IntegerType())), StructField("ls", ArrayType(StringType())),
    StructField("m", MapType(StringType(), IntegerType())), StructField("lst", ArrayType(StructType([StructField("a", LongType()), StructField("b", StringType())]))),
])
def row(i, p):
    return (n(p, lambda: rng.random() < .5), n(p, lambda: rng.randint(-128, 127)), n(p, lambda: rng.randint(-32768, 32767)),
            n(p, lambda: rng.randint(-2**31, 2**31 - 1)), n(p, lambda: rng.randint(-2**63, 2**63 - 1)),
            n(p, lambda: rng.uniform(-1e6, 1e6)), n(p, lambda: rng.uniform(-1e12, 1e12)),
            n(p, lambda: rng.choice(["a", "bb", "", "k%d" % rng.randint(0, 9), "x" * rng.randint(0, 40)])), n(p, lambda: "v%d" % rng.randint(0, 5)),
            n(p, lambda: bytearray(rng.randrange(256) for _ in range(rng.randint(0, 9)))),
            n(p, lambda: datetime.date(1970, 1, 1) + datetime.timedelta(days=rng.randint(-20000, 40000))),
            n(p, lambda: datetime.datetime(1990, 1, 1) + datetime.timedelta(seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999))),
            n(p, lambda: datetime.datetime(1990, 1, 1) + datetime.timedelta(seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999))),
            n(p, lambda: decimal.Decimal(rng.randint(-10**9, 10**9)).scaleb(-2)), n(p, lambda: decimal.Decimal(rng.randint(-10**28, 10**28)).scaleb(-5)),
            n(p, lambda: decimal.Decimal(rng.randint(-10**30, 10**30)).scaleb(-18)),
            n(p, lambda: (n(p, lambda: rng.randint(0, 9)), n(p, lambda: "z"))),
            n(p, lambda: [n(p, lambda: rng.randint(0, 99)) for _ in range(rng.randint(0, 4))]),
            n(p, lambda: [n(p, lambda: rng.choice(["p", "q", "r%d" % rng.randint(0, 3)])) for _ in range(rng.randint(0, 5))]),
            n(p, lambda: {"k%d" % j: n(p, lambda: j) for j in range(rng.randint(0, 3))}),
            n(p, lambda: [n(p, lambda: (n(p, lambda: rng.randint(0, 5)), n(p, lambda: "e"))) for _ in range(rng.randint(0, 3))]))
configs = []
for k, comp in enumerate(["none", "snappy", "zlib", "lz4", "zstd", "lzo"]):
    for rows, p, extra in [(1, 0.0, {}), (5000, 0.2, {}), (20000, 0.0, {"orc.stripe.size": "300000", "orc.row.index.stride": "1000"}),
                           (3000, 0.9, {"orc.bloom.filter.columns": "i,str,l,dec"}), (0, 0.0, {})]:
        configs.append((comp, rows, p, extra))
for k, (comp, rows, p, extra) in enumerate(configs):
    data = [row(i, p) for i in range(rows)]
    df = spark.createDataFrame(data, schema).repartition(2 if rows > 1000 else 1)
    w = df.write.mode("overwrite").option("compression", comp)
    for a, b in extra.items():
        w = w.option(a, b)
    path = os.path.join(out, "spark_%s_%02d_%s" % (ver, k, comp))
    try:
        w.orc(path)
        print("OK", path, flush=True)
    except Exception as e:
        print("FAIL", path, str(e)[:100].replace("\n", " "), flush=True)
spark.stop()
