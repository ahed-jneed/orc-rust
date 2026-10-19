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
Writes tests/basic/data/decimal_layout_*.orc: a pyorc decimal file in the legacy
layout and copies of it, and of tests/basic/data/decimal64_v2.orc, whose footer,
stream kinds, encodings or postscript version were rewritten to damage the layout
of one decimal column. Every file has one stripe.

usage: generate_orc_decimal_layout.py DATA_DIR
"""

import sys
import zlib
from decimal import Decimal

import pyorc

PRESENT, DATA, LENGTH, SECONDARY, ROW_INDEX = 0, 1, 2, 5, 6
DIRECT, DIRECT_V2, DICTIONARY_V2 = 0, 2, 3
DECIMAL = 14


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


def parse(buf):
    fields = []
    i = 0
    while i < len(buf):
        key, i = read_varint(buf, i)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, i = read_varint(buf, i)
        elif wire == 2:
            n, i = read_varint(buf, i)
            value = buf[i : i + n]
            i += n
        elif wire == 1:
            value = buf[i : i + 8]
            i += 8
        elif wire == 5:
            value = buf[i : i + 4]
            i += 4
        else:
            raise ValueError(wire)
        fields.append([number, wire, value])
    return fields


def encode(fields):
    out = bytearray()
    for number, wire, value in fields:
        out += varint(number << 3 | wire)
        if wire == 0:
            out += varint(value)
        elif wire == 2:
            out += varint(len(value)) + value
        else:
            out += value
    return bytes(out)


def get(fields, number):
    return [f for f in fields if f[0] == number]


def unwrap(section, compressed):
    if not compressed:
        return section
    out = b""
    i = 0
    while i < len(section):
        header = section[i] | section[i + 1] << 8 | section[i + 2] << 16
        i += 3
        chunk = section[i : i + (header >> 1)]
        i += header >> 1
        out += chunk if header & 1 else zlib.decompress(chunk, -15)
    return out


def wrap(raw, compressed):
    if not compressed:
        return raw
    assert len(raw) < 1 << 22
    return (len(raw) << 1 | 1).to_bytes(3, "little") + raw


def rewrite(data, version=None, stripe_footer=None, types=None):
    ps_len = data[-1]
    ps = parse(data[-1 - ps_len : -1])
    compressed = get(ps, 2)[0][2] != 0
    footer_len = get(ps, 1)[0][2]
    meta = get(ps, 5)
    meta_len = meta[0][2] if meta else 0
    footer_end = len(data) - 1 - ps_len
    footer = parse(unwrap(data[footer_end - footer_len : footer_end], compressed))
    stripes = get(footer, 3)
    assert len(stripes) == 1
    info = parse(stripes[0][2])
    offset = get(info, 1)[0][2]
    index_len = get(info, 2)[0][2]
    data_len = get(info, 3)[0][2]
    sf_len = get(info, 4)[0][2]
    sf_start = offset + index_len + data_len
    assert sf_start + sf_len == footer_end - footer_len - meta_len
    sf = parse(unwrap(data[sf_start : sf_start + sf_len], compressed))
    if stripe_footer:
        stripe_footer(sf)
    new_sf = wrap(encode(sf), compressed)
    get(info, 4)[0][2] = len(new_sf)
    stripes[0][2] = encode(info)
    if types:
        types(get(footer, 4))
    new_footer = wrap(encode(footer), compressed)
    get(ps, 1)[0][2] = len(new_footer)
    if version:
        get(ps, 4)[0][2] = b"".join(varint(v) for v in version)
    new_ps = encode(ps)
    assert len(new_ps) < 256
    metadata = data[footer_end - footer_len - meta_len : footer_end - footer_len]
    return data[:sf_start] + new_sf + metadata + new_footer + new_ps + bytes([len(new_ps)])


def streams(sf):
    return [parse(f[2]) for f in sf if f[0] == 1]


def set_streams(sf, edit):
    parsed = streams(sf)
    edit(parsed)
    out = []
    placed = False
    for f in sf:
        if f[0] == 1:
            if not placed:
                out += [[1, 2, encode(s)] for s in parsed]
                placed = True
        else:
            out.append(f)
    sf[:] = out


def stream_kind(stream):
    return get(stream, 1)[0]


def stream_column(stream):
    column = get(stream, 2)
    return column[0][2] if column else 0


def restream(column, kind, new_kind):
    def edit(parsed):
        hits = [s for s in parsed if stream_column(s) == column and stream_kind(s)[2] == kind]
        assert len(hits) == 1, (column, kind, len(hits))
        stream_kind(hits[0])[2] = new_kind

    return lambda sf: set_streams(sf, edit)


def add_stream(column, kind, length):
    def edit(parsed):
        parsed.append(
            [[1, 0, kind], [2, 0, column], [3, 0, length]]
        )

    return lambda sf: set_streams(sf, edit)


def reencode(column, kind):
    def edit(sf):
        columns = get(sf, 2)
        parse_col = parse(columns[column][2])
        get(parse_col, 1)[0][2] = kind
        columns[column][2] = encode(parse_col)

    return edit


def retype(column, precision, scale):
    def edit(type_fields):
        t = parse(type_fields[column][2])
        assert get(t, 1)[0][2] == DECIMAL
        get(t, 5)[0][2] = precision
        get(t, 6)[0][2] = scale
        type_fields[column][2] = encode(t)

    return edit


def write_pyorc(path, schema, rows):
    with open(path, "wb") as f:
        with pyorc.Writer(f, schema, compression=pyorc.CompressionKind.NONE) as w:
            for row in rows:
                w.write(row)


def main(out):
    def emit(name, content):
        with open(f"{out}/decimal_layout_{name}.orc", "wb") as f:
            f.write(content)

    legacy = f"{out}/decimal_layout_legacy.orc"
    write_pyorc(
        legacy,
        "struct<d:decimal(5,2)>",
        [(Decimal("0.34"),), (Decimal("-1.50"),), (Decimal("99.99"),)],
    )
    legacy_data = open(legacy, "rb").read()
    bloom = f"{out}/decimal_layout_bloom_filter.orc"
    with open(bloom, "wb") as f:
        with pyorc.Writer(
            f,
            "struct<d:decimal(5,2)>",
            compression=pyorc.CompressionKind.NONE,
            bloom_filter_columns=["d"],
        ) as w:
            for v in ["0.34", "-1.50", "99.99"]:
                w.write((Decimal(v),))
    emit("legacy_secondary_as_data", rewrite(legacy_data, stripe_footer=restream(1, SECONDARY, DATA)))
    emit("legacy_extra_secondary", rewrite(legacy_data, stripe_footer=restream(1, ROW_INDEX, SECONDARY)))
    emit("legacy_extra_data", rewrite(legacy_data, stripe_footer=restream(1, ROW_INDEX, DATA)))
    emit("legacy_extra_length", rewrite(legacy_data, stripe_footer=restream(1, ROW_INDEX, LENGTH)))
    emit("legacy_data_as_secondary", rewrite(legacy_data, stripe_footer=restream(1, DATA, SECONDARY)))
    emit("legacy_secondary_missing", rewrite(legacy_data, stripe_footer=restream(1, SECONDARY, LENGTH)))
    emit("legacy_decimal64_version", rewrite(legacy_data, version=[1, 9999]))

    wide = f"{out}/decimal_layout_scale_source.orc"
    write_pyorc(wide, "struct<d:decimal(12,2)>", [(Decimal("0.24"),)])
    wide_data = open(wide, "rb").read()
    for scale in [13, 127, 128, 255, 256]:
        emit(f"scale_{scale}", rewrite(wide_data, types=retype(1, 12, scale)))
    emit("precision_39", rewrite(wide_data, types=retype(1, 39, 2)))
    emit("precision_256", rewrite(wide_data, types=retype(1, 256, 2)))
    emit("precision_unspecified_scale_128", rewrite(wide_data, types=retype(1, 0, 128)))
    emit("scale_above_precision", rewrite(wide_data, types=retype(1, 12, 13)))

    with open(f"{out}/decimal64_v2.orc", "rb") as f:
        d64 = f.read()
    # column 2 is decimal(12,0), column 4 is decimal(12,2), column 3 is decimal(20,2)
    emit("d64_extra_secondary", rewrite(d64, stripe_footer=restream(2, ROW_INDEX, SECONDARY)))
    emit("d64_extra_data", rewrite(d64, stripe_footer=restream(2, ROW_INDEX, DATA)))
    emit("d64_extra_length", rewrite(d64, stripe_footer=restream(2, ROW_INDEX, LENGTH)))
    emit("d64_data_as_secondary", rewrite(d64, stripe_footer=restream(2, DATA, SECONDARY)))
    emit("d64_data_as_length", rewrite(d64, stripe_footer=restream(2, DATA, LENGTH)))
    emit("d64_dictionary_encoding", rewrite(d64, stripe_footer=reencode(2, DICTIONARY_V2)))
    emit("d64_legacy_version", rewrite(d64, version=[0, 12]))
    emit("d64_wide_extra_data", rewrite(d64, stripe_footer=restream(3, ROW_INDEX, DATA)))
    emit("d64_scale_128", rewrite(d64, types=retype(2, 12, 128)))
    emit("d64_scale_256", rewrite(d64, types=retype(2, 12, 256)))
    emit("d64_scale_13", rewrite(d64, types=retype(4, 12, 13)))


if __name__ == "__main__":
    main(sys.argv[1])
