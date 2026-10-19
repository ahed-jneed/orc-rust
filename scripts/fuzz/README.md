# Byte-mutation fuzzing of the Arrow reader

Dev tool, not part of CI. Mutates seed ORC files (bit flips, byte sets, truncation,
chunk duplication and deletion in the postscript, footer, stripe footers, index and
data streams, and the 3-byte compression chunk headers) and reads each mutant with the
Arrow reader at batch sizes 1 and 8192, with all columns and a single-column projection.

```sh
pip install pyorc
python3 scripts/fuzz/gen_seeds.py /tmp/orc-seeds
CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true \
  cargo build --release --example fuzz_mutate
target/release/examples/fuzz_mutate /tmp/orc-seeds /tmp/orc-fuzz-out 1 25000
```

Arguments: seed directory, output directory, RNG seed, number of mutants. Mutants are
read in supervised worker processes with `ulimit -v 4000000` and a 30 s hang timeout.
Panics, aborts, OOMs and hangs are reported with the offending mutant saved in the
output directory. `diff` counts mutants that decoded without error to different values
than the seed.

The driver refuses to start when a seed does not decode: every `ok` mutant of such a seed
would otherwise count as `diff`. The seeds have no null rows at the top level (a null row
of the root struct makes the reader fail on some files).

The summary also prints `CLASS <region> <status>: <n>` lines, the mutants per byte region
(`magic`, `postscript`, `metadata`, `footer`, `stripe_index`, `stripe_data_plain`,
`stripe_data_compressed`, `stripe_footer`, `stream_header`), and saves every `diff` mutant as
`diff_<rng-seed>_<n>.orc` in the output directory. Classify them by byte region and by the
metadata fields that changed:

```sh
pip install cramjam lz4
python3 scripts/fuzz/classify.py /tmp/orc-seeds /tmp/orc-fuzz-out
```

A `diff` mutant that changed no metadata field only changed payload bytes: nothing in the
file can prove that damage.

## Comparing two builds

`gen_corpus.py` writes valid files with pyorc and pyarrow. Decode them, and any other
files, with two builds and compare the output:

```sh
python3 scripts/fuzz/gen_corpus.py /tmp/orc-corpus
target/release/examples/fuzz_mutate hash /tmp/orc-corpus/*.orc > new.txt
```

Each line is the path and a hash of the decoded values, or the error.
