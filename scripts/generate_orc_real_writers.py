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
Writes small ORC files with Apache ORC C++ (through pyarrow and pyorc) that other
readers accept and that look unusual in their metadata, together with the rows pyarrow
reads back, to be checked by tests/basic/real_writers.rs. The files written by Apache
ORC Java come from generate_orc_java_corpus.sh; the expected rows are added to them with
the --expected option.

usage: generate_orc_real_writers.py OUT_DIR
       generate_orc_real_writers.py --expected FILE...

The expected file has the format of generate_orc_null_matrix.py.

The statistics ORC C++ writes for the int variant of a union are uninitialized memory
(a pointer in the files below), so the file written by the union case differs from run
to run.
"""

import io
import os
import sys

import pyarrow as pa
import pyarrow.orc as paorc
import pyorc

import generate_orc_null_matrix as matrix


def write_expected(path):
    lines = matrix.pyarrow_lines(path)
    assert lines is not None, path
    names = paorc.read_table(path).column_names
    with open(path.replace(".orc", ".expected"), "w") as f:
        f.write("\t".join(names) + "\n")
        for line in lines:
            f.write(line + "\n")


def pyarrow_list_of_strings(out_dir):
    table = pa.table(
        {
            "l": pa.array([["a", "b", "c"], ["a"]], pa.list_(pa.string())),
            "n": pa.array([1, 2], pa.int32()),
        }
    )
    path = os.path.join(out_dir, "cpp_pyarrow_list_of_strings_dictionary.orc")
    paorc.write_table(table, path, dictionary_key_size_threshold=1.0, compression="zlib")
    return path


def pyorc_list_of_strings(out_dir):
    path = os.path.join(out_dir, "cpp_pyorc_list_of_strings_dictionary.orc")
    with open(path, "wb") as f:
        writer = pyorc.Writer(
            f, "struct<l:array<string>,m:map<string,string>>", dict_key_size_threshold=1.0
        )
        writer.write((["x", "y", "z"], {"p": "q", "r": "s"}))
        writer.close()
    return path


def varint(data, at):
    value = shift = 0
    while True:
        byte = data[at]
        at += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, at


def fields(data):
    at = 0
    while at < len(data):
        key, at = varint(data, at)
        if key & 7 == 0:
            value, at = varint(data, at)
        elif key & 7 == 2:
            length, at = varint(data, at)
            value = data[at : at + length]
            at += length
        else:
            raise AssertionError(key)
        yield key >> 3, value


def has_int_statistics_beyond_int(data):
    """Whether the uncompressed file tail holds integer statistics beyond 32 bits."""
    postscript_length = data[-1]
    footer_length = next(v for f, v in fields(data[-1 - postscript_length : -1]) if f == 1)
    footer = data[-1 - postscript_length - footer_length : -1 - postscript_length]
    for field, statistics in fields(footer):
        if field != 7:
            continue
        for kind, integer in fields(statistics):
            if kind == 2:
                for _, zigzag in fields(integer):
                    if abs((zigzag >> 1) ^ -(zigzag & 1)) >= 2**31:
                        return True
    return False


def pyorc_union_statistics(out_dir):
    path = os.path.join(out_dir, "cpp_pyorc_union_with_null_int_variant.orc")
    for _ in range(20):
        buf = io.BytesIO()
        writer = pyorc.Writer(
            buf,
            "struct<u:uniontype<int,string>>",
            compression=pyorc.CompressionKind.NONE,
        )
        for row in [(5,), (None,), ("a",)]:
            writer.write(row)
        writer.close()
        if has_int_statistics_beyond_int(buf.getvalue()):
            break
    else:
        raise SystemExit("pyorc wrote no garbage statistics")
    with open(path, "wb") as f:
        f.write(buf.getvalue())
    return path


def main(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for make in (pyarrow_list_of_strings, pyorc_list_of_strings, pyorc_union_statistics):
        path = make(out_dir)
        write_expected(path)
        print("%s: %d bytes" % (os.path.basename(path), os.path.getsize(path)))


if __name__ == "__main__":
    if sys.argv[1] == "--expected":
        for file in sys.argv[2:]:
            write_expected(file)
    else:
        main(sys.argv[1])
