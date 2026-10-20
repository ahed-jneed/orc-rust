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
Prints the writer options and the schema of every file generate_orc_java_corpus.sh has
Apache ORC Java write, one tab separated line per file, for one version of ORC:
name, schema, codec, stripe size, row index stride, block padding, bloom filter columns,
file version, rows, seed, null probability, encoding strategy, batch size.

usage: corpus_spec.py VERSION
"""

import random
import sys

ver = sys.argv[1]
old = ver.startswith("1.5")
ALL = ("b:boolean,t:tinyint,s:smallint,i:int,l:bigint,f:float,d:double,str:string,v:varchar(10),c:char(5),bin:binary,dt:date,ts:timestamp,"
       + ("" if old else "tsi:timestamp with local time zone,") + "dec:decimal(10,2),dec2:decimal(30,5),dec3:decimal(18,4),dec4:decimal(38,10),dec5:decimal(1,0)")
SCHEMAS = {
 "all": "struct<%s>" % ALL,
 "nested": "struct<a:struct<b:struct<c:int,d:string>,e:array<struct<f:bigint,g:string>>>,m:map<string,struct<x:int>>,u:uniontype<struct<p:int>,array<string>>,k:int>",
 "lists": "struct<l1:array<int>,l2:array<array<string>>,m1:map<int,array<double>>,m2:map<string,string>,t:array<timestamp>,d:array<decimal(20,3)>>",
 "union": "struct<u1:uniontype<int,string>,u2:uniontype<boolean,bigint,double>,u3:uniontype<struct<a:int,b:string>,array<int>,map<string,int>>,k:int>",
 "strings": "struct<s1:string,s2:string,v:varchar(5),c:char(3),b:binary>",
}
CODECS = ["NONE", "ZLIB", "SNAPPY", "LZ4", "ZSTD", "LZO"]
rng = random.Random("corpus" + ver)
lines = []
n = 0
for sname, schema in SCHEMAS.items():
    for k in range(14):
        codec = CODECS[k % len(CODECS)] if k < 12 else rng.choice(CODECS)
        multi = rng.random() < 0.5
        stripe = 150000 if multi else 67108864
        stride = rng.choice([10000, 1000, 0, 5000])
        padding = rng.choice(["true", "false"])
        bloom = ""
        if sname == "all" and rng.random() < 0.5:
            bloom = "i,str,l,dec,dt,ts"
        elif sname == "strings" and rng.random() < 0.5:
            bloom = "s1,v,c"
        fv = rng.choice(["", "0.11", "0.12"])
        rows = rng.choice([20000, 8000, 3000, 1, 0, 1025, 10001])
        nullp = rng.choice([0.0, 0.0, 0.2, 0.5, 0.9, 1.0])
        strategy = rng.choice(["SPEED", "COMPRESSION"])
        batch = rng.choice([1024, 1024, 64, 5000])
        name = "%s_%s_%02d_%s" % (ver, sname, k, codec.lower())
        lines.append("\t".join([name, schema, codec, str(stripe), str(stride), padding, bloom, fv, str(rows), str(rng.randrange(10**6)), str(nullp), strategy, str(batch)]))
print("\n".join(lines))
