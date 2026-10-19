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
Writes small ORC files with a writer timezone, using pyorc, together with the
wall-clock values pyarrow reads from them, to be checked by
tests/basic/timestamp_zones.rs.

usage: generate_orc_timestamp_zones.py OUT_DIR

Every case is OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected, whose first line is
the writer timezone and whose other lines are the pyarrow values as ISO
timestamps, except the rows at the end, whose values come from zoneinfo:
the fractional ones because pyarrow reads a negative instant with a fraction
a second late, the ones before 1900 because the reader keeps the zone's local
mean time there for files written by implementations other than ORC Java. The instants written cover both sides of every transition from
2100 to 2200 and a mid month sample of every month in 2100 and 2200, then
three instants before 1900 and fractional seconds around the first two
transitions between 1916 and 1969.
"""

import sys
import zoneinfo
from datetime import datetime, timezone

import pyarrow as pa
from pyarrow import orc
import pyorc

ZONES = [
    "America/Los_Angeles",
    "Europe/Berlin",
    "Australia/Sydney",
    "Asia/Kolkata",
    "America/Sao_Paulo",
    "America/Mexico_City",
    "Europe/Moscow",
    "Europe/Dublin",
    "Pacific/Auckland",
    "America/Santiago",
    "America/Havana",
    "America/Asuncion",
    "Asia/Beirut",
    "Africa/Cairo",
]


class RawTimestamp:
    @staticmethod
    def from_orc(obj, tz):
        return obj

    @staticmethod
    def to_orc(obj, tz):
        return obj


def utc(*args):
    return int(datetime(*args, tzinfo=timezone.utc).timestamp())


def transitions(zone, first_year, last_year):
    tz = zoneinfo.ZoneInfo(zone)

    def offset(t):
        return datetime.fromtimestamp(t, tz).utcoffset()

    out = []
    t = utc(first_year, 1, 1)
    end = utc(last_year + 1, 1, 1)
    while t < end:
        if offset(t) != offset(t + 3600):
            low, high = t, t + 3600
            while high - low > 1:
                mid = (low + high) // 2
                if offset(mid) == offset(low):
                    low = mid
                else:
                    high = mid
            out.append(high)
        t += 3600
    return out


def fractional(zone):
    out = [(utc(*d), 0) for d in [(1800, 1, 1), (1880, 1, 1), (1899, 12, 31, 23)]]
    for t in transitions(zone, 1916, 1969)[:2]:
        out += [(t - 1, 500_000_000), (t - 1, 999_000_000), (t, 0), (t, 1_000_000)]
    return out


def instants(zone):
    out = [utc(2099, 12, 31, 23, 59, 59), utc(2100, 1, 1)]
    for year in (2100, 2200):
        for month in range(1, 13):
            out.append(utc(year, month, 15, 12, 34, 56))
    for year in list(range(2100, 2200)) + [2200]:
        for t in transitions(zone, year, year):
            out += [t - 1, t, t + 1]
    out.append(utc(2250, 7, 1, 6))
    return sorted(set(out))


def main(out_dir):
    for zone in ZONES:
        name = zone.replace("/", "_").lower()
        values = instants(zone)
        path = f"{out_dir}/{name}.orc"
        with open(path, "wb") as f:
            with pyorc.Writer(
                f,
                "struct<t:timestamp>",
                timezone=zoneinfo.ZoneInfo(zone),
                converters={pyorc.TypeKind.TIMESTAMP: RawTimestamp},
            ) as writer:
                for v in values:
                    writer.write(((v, 0),))
                for v in fractional(zone):
                    writer.write((v,))
        table = orc.read_table(path)
        walls = [wall.isoformat() for wall in table.column(0).to_pylist()]
        assert len(walls) == len(values) + len(fractional(zone))
        tz = zoneinfo.ZoneInfo(zone)
        for i, (seconds, nanos) in enumerate(fractional(zone), len(values)):
            wall = datetime.fromtimestamp(seconds, tz).replace(tzinfo=None)
            walls[i] = wall.strftime("%Y-%m-%dT%H:%M:%S") + (f".{nanos:09d}" if nanos else "")
        with open(f"{out_dir}/{name}.expected", "w") as f:
            f.write(zone + "\n")
            for wall in walls:
                f.write(wall + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
