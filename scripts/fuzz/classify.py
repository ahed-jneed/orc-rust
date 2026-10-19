#!/usr/bin/env python3
"""Classify the `diff` mutants saved by fuzz_mutate by byte region and by changed metadata field.

Usage: python3 scripts/fuzz/classify.py <seed-dir> <out-dir>...
Requires: pip install cramjam lz4

Every `diff_<rng-seed>_<n>.orc` in an output directory is compared with the seed file it was
mutated from (seed number `n % number-of-seeds` in sorted order, as fuzz_mutate picks it). The
postscript, footer, metadata and stripe footers of both files are decoded as protobuf and the
first differing byte is mapped to a region of the seed. A mutant whose sections decode and
differ in no field changed only payload bytes.
"""
import collections
import glob
import os
import re
import sys

import cramjam
import lz4.block

POSTSCRIPT = {1: "footerLength", 2: "compression", 3: "compressionBlockSize", 4: "version",
              5: "metadataLength", 6: "writerVersion", 8000: "magic"}
STRIPE_INFO = {1: "offset", 2: "indexLength", 3: "dataLength", 4: "footerLength",
               5: "numberOfRows"}
TYPE = {1: "kind", 2: "subtypes", 3: "fieldNames", 4: "maximumLength", 5: "precision", 6: "scale"}
FOOTER = {1: "headerLength", 2: "contentLength", 3: "stripes", 4: "types", 5: "metadata",
          6: "numberOfRows", 7: "statistics", 8: "rowIndexStride", 9: "writer", 10: "encryption",
          11: "calendar", 12: "softwareVersion"}
STREAM = {1: "kind", 2: "column", 3: "length"}
ENCODING = {1: "kind", 2: "dictionarySize", 3: "bloomEncoding"}
STRIPE_FOOTER = {1: "streams", 2: "columns", 3: "writerTimezone", 4: "encryption"}


def varint(buf, i):
    value = shift = 0
    while True:
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, i
        shift += 7
        if shift > 70:
            raise ValueError("varint")


def parse(buf):
    out, i = [], 0
    while i < len(buf):
        tag, i = varint(buf, i)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, i = varint(buf, i)
        elif wire == 2:
            n, i = varint(buf, i)
            if i + n > len(buf):
                raise ValueError("length")
            value, i = bytes(buf[i:i + n]), i + n
        elif wire == 1:
            value, i = bytes(buf[i:i + 8]), i + 8
        elif wire == 5:
            value, i = bytes(buf[i:i + 4]), i + 4
        else:
            raise ValueError("wire type")
        out.append((field, value))
    return out


def chunks(buf):
    pos = 0
    while pos + 3 <= len(buf):
        raw = buf[pos] | buf[pos + 1] << 8 | buf[pos + 2] << 16
        n = raw >> 1
        yield pos, raw & 1, buf[pos + 3:pos + 3 + n]
        pos += 3 + n


def decompress(buf, codec, block):
    if codec == 0:
        return bytes(buf)
    out = bytearray()
    for _, original, body in chunks(buf):
        if original:
            out += body
        elif codec == 1:
            out += bytes(cramjam.deflate.decompress(body))
        elif codec == 2:
            out += bytes(cramjam.snappy.decompress_raw(body))
        elif codec == 4:
            out += bytes(lz4.block.decompress(body, uncompressed_size=block))
        elif codec == 5:
            out += bytes(cramjam.zstd.decompress(body))
        else:
            raise ValueError("codec")
    return bytes(out)


def name_fields(fields, names, prefix, into):
    counts = collections.Counter()
    for field, value in fields:
        key = f"{prefix}.{names.get(field, f'field{field}')}"
        into[(key, counts[key])] = value
        counts[key] += 1


def view(data):
    """Maps (path, occurrence) to value for everything the reader takes from the metadata."""
    out = {}
    ps_len = data[-1]
    ps_start = len(data) - 1 - ps_len
    ps = parse(data[ps_start:-1])
    name_fields(ps, POSTSCRIPT, "postscript", out)
    psd = {f: v for f, v in ps}
    codec, block = psd.get(2, 0), psd.get(3, 262144)
    footer_len, metadata_len = psd[1], psd.get(5, 0)
    footer_start = ps_start - footer_len
    footer = parse(decompress(data[footer_start:ps_start], codec, block))
    stripes = []
    types = 0
    for field, value in footer:
        if field == 3:
            info = {}
            name_fields(parse(value), STRIPE_INFO, f"footer.stripes[{len(stripes)}]", out)
            for f, v in parse(value):
                info[f] = v
            stripes.append(info)
        elif field == 4:
            name_fields(parse(value), TYPE, f"footer.types[{types}]", out)
            types += 1
    name_fields([(f, v) for f, v in footer if f not in (3, 4, 7)], FOOTER, "footer", out)
    out[("footer.statistics", 0)] = b"".join(v for f, v in footer if f == 7)
    out[("metadata", 0)] = decompress(
        data[footer_start - metadata_len:footer_start], codec, block)
    geometry = []
    for n, info in enumerate(stripes):
        offset, index, body, tail = (info.get(f, 0) for f in (1, 2, 3, 4))
        geometry.append((offset, offset + index, offset + index + body,
                         offset + index + body + tail))
        raw = data[offset + index + body:offset + index + body + tail]
        sf = parse(decompress(raw, codec, block))
        streams = encodings = 0
        for field, value in sf:
            if field == 1:
                name_fields(parse(value), STREAM, f"stripe_footer[{n}].streams[{streams}]", out)
                streams += 1
            elif field == 2:
                name_fields(parse(value), ENCODING, f"stripe_footer[{n}].columns[{encodings}]",
                            out)
                encodings += 1
            else:
                out[(f"stripe_footer[{n}].field{field}", 0)] = value
    return out, geometry, (footer_start - metadata_len, footer_start, ps_start), codec, block


def region(offset, geometry, tail, codec, seed):
    metadata_start, footer_start, ps_start = tail
    if offset < 3:
        return "magic"
    for n, (start, index_end, data_end, end) in enumerate(geometry):
        if start <= offset < end:
            part = ("index" if offset < index_end else "data" if offset < data_end else "footer")
            if part == "footer":
                return "stripe footer"
            if codec:
                base = start if part == "index" else index_end
                body = seed[base:index_end if part == "index" else data_end]
                for pos, _, payload in chunks(body):
                    if pos <= offset - base < pos + 3:
                        return f"stripe {part} chunk header"
                return f"stripe {part} payload (compressed)"
            return f"stripe {part} payload"
    if metadata_start <= offset < footer_start:
        return "metadata"
    if footer_start <= offset < ps_start:
        return "footer"
    return "postscript"


def generalise(path):
    out, depth = "", 0
    for ch in path:
        if ch == "[":
            depth += 1
            out += "[]"
        elif ch == "]":
            depth -= 1
        elif depth == 0:
            out += ch
    return re.sub(r"\bfield\d+", "unknown field", out)


def classify(seed, mutant):
    first = next((i for i, (a, b) in enumerate(zip(seed, mutant)) if a != b),
                 min(len(seed), len(mutant)))
    try:
        seed_view, geometry, tail, codec, block = view(seed)
    except Exception as e:
        raise SystemExit(f"cannot parse the seed: {e}")
    where = region(first, geometry, tail, codec, seed)
    try:
        mutant_view = view(mutant)[0]
    except Exception:
        return where, "section no longer decodes"
    if len(seed) != len(mutant):
        where += " (file length changed)"
    changed = sorted({generalise(k[0]) for k in set(seed_view) | set(mutant_view)
                      if seed_view.get(k) != mutant_view.get(k)})
    if not changed:
        return where, "payload bytes only" if len(seed) == len(mutant) else (
            "payload bytes inserted or deleted")
    return where, ", ".join(changed)


def main(seed_dir, out_dirs):
    seeds = sorted(glob.glob(os.path.join(seed_dir, "*.orc")))
    table = collections.Counter()
    for out_dir in out_dirs:
        for path in sorted(glob.glob(os.path.join(out_dir, "diff_*.orc"))):
            n = int(os.path.basename(path).rsplit("_", 1)[1][:-4])
            seed = open(seeds[n % len(seeds)], "rb").read()
            mutant = open(path, "rb").read()
            table[classify(seed, mutant)] += 1
    print("| count | byte region | changed |")
    print("|---:|---|---|")
    for (where, changed), count in sorted(table.items(), key=lambda kv: (kv[0][0], -kv[1])):
        print(f"| {count} | {where} | {changed} |")
    print(f"| {sum(table.values())} | total | |")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
