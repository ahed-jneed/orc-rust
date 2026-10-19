#!/usr/bin/env bash
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

# Writes ORC files with Apache ORC Java, for check_orc_corpus.py: every codec, stripe
# size, row index stride, block padding, bloom filter, file version and encoding
# strategy, with nulls in every column, for each ORC version given.
#
# usage: generate_orc_java_corpus.sh WORK_DIR OUT_DIR [VERSION...]
#
# The orc-tools uber jars are downloaded from Maven Central into WORK_DIR, which needs
# java and javac.

set -euo pipefail

work=$1
out=$2
shift 2
versions=("$@")
if [ ${#versions[@]} -eq 0 ]; then
  versions=(1.5.13 1.6.14 1.9.0 1.9.9 2.0.0 2.0.7 2.1.0 2.1.4 2.2.2 2.3.1)
fi
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$work" "$out"

for version in "${versions[@]}"; do
  jar="$work/orc-tools-$version-uber.jar"
  if [ ! -f "$jar" ]; then
    curl -sSf -o "$jar" \
      "https://repo1.maven.org/maven2/org/apache/orc/orc-tools/$version/orc-tools-$version-uber.jar"
  fi
  instant=1
  case $version in
    1.5.* | 1.6.*)
      vectors=org.apache.hadoop.hive.ql.exec.vector
      storage=org.apache.hadoop.hive
      if [[ $version == 1.5.* ]]; then
        instant=0
      fi
      ;;
    *)
      vectors=org.apache.orc.storage.ql.exec.vector
      storage=org.apache.orc.storage
      ;;
  esac
  source="$work/src-$version"
  classes="$work/classes-$version"
  mkdir -p "$source" "$classes"
  sed "s|ORC_VECTOR_PKG|$vectors|; s|ORC_STORAGE_PKG|$storage|g" \
    "$here/java/GenOrcCorpus.java.in" >"$source/GenOrcCorpus.java"
  if [ $instant -eq 0 ]; then
    sed -i '/case TIMESTAMP_INSTANT:/d' "$source/GenOrcCorpus.java"
  fi
  javac -nowarn -cp "$jar" -d "$classes" "$source/GenOrcCorpus.java"
  python3 "$here/java/corpus_spec.py" "$version" >"$work/spec-$version.tsv"
  java --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$jar:$classes" GenOrcCorpus "$out/$version" "$work/spec-$version.tsv" </dev/null |
    grep -v '^OK' || true
  find "$out/$version" -name '.*.crc' -delete
done
