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
Writes tests/basic/data/pyorc_decimal_matrix.orc with pyorc: one decimal column
per precision 1..38 and scale 0..precision, holding negatives and nulls, and
checks that pyarrow reads back the values tests/basic/decimal_matrix.rs expects.
Also writes the two files whose footer type is patched to an invalid decimal.

usage: generate_orc_decimal_matrix.py OUT_DIR
"""

import sys
from decimal import Decimal

import pyarrow.orc as porc
import pyorc

ROWS = 20


def digits(row, precision):
    if row == ROWS - 1:
        return [9] * precision
    return [((row + 1) * 7 + k * 3) % 10 for k in range(precision)]


def value(row, precision, scale):
    if row % 5 == 4:
        return None
    sign = 1 if row % 3 == 1 else 0
    return Decimal((sign, tuple(digits(row, precision)), -scale))


def main(out):
    columns = [(p, s) for p in range(1, 39) for s in range(p + 1)]
    schema = "struct<%s>" % ",".join(f"p{p}_s{s}:decimal({p},{s})" for p, s in columns)
    path = f"{out}/pyorc_decimal_matrix.orc"
    with open(path, "wb") as f:
        with pyorc.Writer(f, schema) as w:
            for row in range(ROWS):
                w.write(tuple(value(row, p, s) for p, s in columns))
    table = porc.read_table(path)
    for p, s in columns:
        got = table.column(f"p{p}_s{s}").to_pylist()
        want = [value(row, p, s) for row in range(ROWS)]
        assert got == want, (p, s)

    for name, precision, scale in [("p39", 39, 2), ("scale_gt_precision", 5, 7)]:
        path = f"{out}/pyorc_decimal_invalid_{name}.orc"
        with open(path, "wb") as f:
            with pyorc.Writer(
                f, "struct<d:decimal(5,2)>", compression=pyorc.CompressionKind.NONE
            ) as w:
                w.write((Decimal("1.50"),))
        data = open(path, "rb").read()
        old = bytes([0x08, 0x0E, 0x20, 0x00, 0x28, 0x05, 0x30, 0x02])
        assert data.count(old) == 1
        new = bytes([0x08, 0x0E, 0x20, 0x00, 0x28, precision, 0x30, scale])
        open(path, "wb").write(data.replace(old, new))


if __name__ == "__main__":
    main(sys.argv[1])
