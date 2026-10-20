## Generate data

Setup the virtual environment with dependencies on PyArrow, PySpark and PyOrc
to generate the reference data:

```bash
# Run once
./scripts/setup-venv.sh
./scripts/prepare-test-data.sh
```

Then execute the tests:

```bash
cargo test
```


## Files from other writers

`generate_orc_null_matrix.py` and `generate_orc_real_writers.py` write the small files
that tests/basic/null_matrix.rs and tests/basic/real_writers.rs read, with the rows
pyarrow and pyorc read from them.

`generate_orc_java_corpus.sh`, `generate_orc_cpp_corpus.py` and
`generate_orc_spark_corpus.py` write larger corpora with Apache ORC Java (1.5 to 2.3),
Apache ORC C++ (pyarrow, pyorc) and Apache Spark. `check_orc_corpus.py` decodes a corpus
with several builds of `examples/dump_rows.rs` and with pyarrow, to find the files that an
older build reads and a newer build refuses:

```bash
cargo build --release --example dump_rows
python3 scripts/check_orc_corpus.py /tmp/corpus old=/path/to/old/dump_rows new=target/release/examples/dump_rows
```
