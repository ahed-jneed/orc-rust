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
