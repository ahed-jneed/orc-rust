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
Writes small ORC files with the ORC Java tools under a given JVM timezone, and
the wall-clock values the ORC Java reader prints for them, to be checked by
tests/basic/timestamp_zones.rs.

usage: generate_orc_timestamp_zones_java.py OUT_DIR ORC_TOOLS_UBER_JAR

The jar is orc-tools-VERSION-uber.jar from Maven Central. Every case is
OUT_DIR/NAME.orc plus OUT_DIR/NAME.expected, whose first line is the writer
timezone and whose other lines are the values printed by `orc-tools data`.
"""

import json
import os
import subprocess
import sys
import tempfile
import zoneinfo
from datetime import datetime

from pyarrow import orc

ZONES = [
    "Asia/Kolkata",
    "America/Los_Angeles",
    "Europe/Berlin",
    "Australia/Sydney",
    "Africa/Abidjan",
    "Europe/Dublin",
]

WALLS = [
    "1800-01-01 00:00:00",
    "1850-06-15 12:00:00",
    "1880-01-01 00:00:00",
    "1890-06-01 12:00:00",
    "1899-12-31 23:00:00",
    "1900-01-01 00:00:00",
    "1900-06-01 12:00:01",
    "1905-06-01 12:00:00",
    "1912-06-01 12:00:00",
    "1920-01-01 00:00:00",
    "1950-06-01 12:00:00",
    "2000-01-01 00:00:00",
    "2024-07-01 12:00:00",
    "2100-01-01 12:00:00",
    "2100-07-01 12:00:00",
    "2200-07-01 12:00:00",
]


def boundary_walls(zone):
    tz = zoneinfo.ZoneInfo(zone)
    return [
        datetime.fromtimestamp(t, tz).strftime("%Y-%m-%d %H:%M:%S")
        for t in (-2208988801, -2208988800)
    ]


def java(zone, jar, *args):
    return subprocess.run(
        ["java", f"-Duser.timezone={zone}", "-jar", jar, *args],
        check=True,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    ).stdout


def main(out_dir, jar):
    with tempfile.TemporaryDirectory() as tmp:
        for zone in ZONES:
            name = zone.replace("/", "_").lower()
            walls = WALLS + boundary_walls(zone)
            source = os.path.join(tmp, "in.json")
            with open(source, "w") as f:
                for wall in walls:
                    f.write(json.dumps({"t": wall}) + "\n")
            path = f"{out_dir}/{name}.orc"
            if os.path.exists(path):
                os.remove(path)
            java(zone, jar, "convert", "-o", path, "-s", "struct<t:timestamp>", source)
            os.remove(f"{out_dir}/.{name}.orc.crc")
            assert orc.ORCFile(path).writer == "ORC_JAVA"
            printed = java(zone, jar, "data", path)
            rows = [json.loads(line)["t"] for line in printed.splitlines() if line.startswith("{")]
            assert len(rows) == len(walls)
            with open(f"{out_dir}/{name}.expected", "w") as f:
                f.write(zone + "\n")
                for row in rows:
                    assert row.endswith(".0")
                    f.write(row[:-2].replace(" ", "T") + "\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
