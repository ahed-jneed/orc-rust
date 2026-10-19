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
Decodes every ORC file under a directory with several builds of the examples/dump_rows
example, and with pyarrow, and reports the files that one build reads and a later build
refuses, and the files whose rows differ from the ones pyarrow reads.

usage: check_orc_corpus.py CORPUS_DIR NAME=DUMP_ROWS_BINARY...

The first build is the baseline: a file it reads and a later build refuses is a
regression. The files are the ones generate_orc_java_corpus.sh, generate_orc_cpp_corpus.py
and generate_orc_spark_corpus.py write.
"""

import collections
import glob
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import generate_orc_null_matrix as matrix


def run(command):
    try:
        done = subprocess.run(
            command,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        return "hang", ""
    if done.returncode != 0:
        return "ERR:" + (done.stderr.strip().split("\n") or [""])[0][:100], ""
    for line in done.stdout.split("\n"):
        if line.startswith("ERROR:"):
            return line[:120], ""
    return "OK", done.stdout.split("\n", 1)[1]


def check(args):
    path, binaries = args
    results = {}
    rows = {}
    for name, binary in binaries.items():
        results[name], rows[name] = run([binary, path, "8192"])
    reference = None
    last = list(binaries)[-1]
    if results[last] == "OK":
        reference = matrix.pyarrow_lines(path)
    differs = reference is not None and rows[last].split("\n")[:-1] != reference
    return path, results, differs, reference is not None


def main(corpus, named):
    binaries = dict(item.split("=", 1) for item in named)
    paths = sorted(glob.glob(os.path.join(corpus, "**", "*.orc"), recursive=True))
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(check, [(path, binaries) for path in paths]))
    names = list(binaries)
    counts = collections.Counter()
    for _, outcome, _, _ in results:
        counts[tuple(outcome[name] == "OK" for name in names)] += 1
    print("files:", len(results))
    for key, count in sorted(counts.items()):
        print("  read by %s: %d" % (", ".join(n for n, ok in zip(names, key) if ok) or "none", count))
    base = names[0]
    for later in names[1:]:
        for path, outcome, _, _ in results:
            if outcome[base] == "OK" and outcome[later] != "OK":
                print("REGRESSION %s %s: %s" % (later, os.path.relpath(path, corpus), outcome[later]))
    for path, _, differs, _ in results:
        if differs:
            print("DIFFERS FROM PYARROW", os.path.relpath(path, corpus))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
