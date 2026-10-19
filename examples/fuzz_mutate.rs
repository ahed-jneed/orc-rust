// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Byte-mutation fuzzer for the Arrow reader. See scripts/fuzz/README.md.
//!
//! `fuzz_mutate <seed-dir> <out-dir> <rng-seed> <count>` mutates seed files and reads every
//! mutant in a supervised worker process, classifying panics, aborts, OOMs and hangs.
//! `fuzz_mutate hash <file>...` prints a hash of the decoded contents of each file.

use std::collections::{hash_map::DefaultHasher, BTreeMap};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::metadata::read_metadata;
use orc_rust::ArrowReaderBuilder;

const HANG_TIMEOUT: Duration = Duration::from_secs(30);
const MEMORY_LIMIT_KB: u64 = 4_000_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

struct Region {
    name: &'static str,
    start: usize,
    end: usize,
}

fn varint_field(buf: &[u8], field: u64) -> Option<u64> {
    let mut i = 0;
    let read = |i: &mut usize| -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = *buf.get(*i)?;
            *i += 1;
            v |= ((b & 0x7F) as u64).checked_shl(shift)?;
            if b & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
        }
    };
    while i < buf.len() {
        let tag = read(&mut i)?;
        match tag & 7 {
            0 => {
                let v = read(&mut i)?;
                if tag >> 3 == field {
                    return Some(v);
                }
            }
            2 => {
                let n = read(&mut i)? as usize;
                i += n;
            }
            1 => i += 8,
            5 => i += 4,
            _ => return None,
        }
    }
    None
}

fn chunk_headers(data: &[u8], start: usize, end: usize, out: &mut Vec<usize>) {
    let mut pos = start;
    while pos + 3 <= end {
        out.push(pos);
        let raw =
            data[pos] as usize | (data[pos + 1] as usize) << 8 | (data[pos + 2] as usize) << 16;
        pos += 3 + (raw >> 1);
    }
}

fn regions(data: &[u8]) -> (Vec<Region>, Vec<usize>) {
    let len = data.len();
    let mut regions = vec![Region {
        name: "magic",
        start: 0,
        end: 3.min(len),
    }];
    let mut headers = vec![];
    let ps_len = *data.last().unwrap() as usize;
    let ps_start = len - 1 - ps_len;
    regions.push(Region {
        name: "postscript",
        start: ps_start,
        end: len,
    });
    let footer_len = varint_field(&data[ps_start..len - 1], 1).unwrap() as usize;
    let footer_start = ps_start - footer_len;
    regions.push(Region {
        name: "footer",
        start: footer_start,
        end: ps_start,
    });
    let mut reader = Bytes::copy_from_slice(data);
    let metadata = read_metadata(&mut reader).unwrap();
    let compressed = metadata.compression().is_some();
    if compressed {
        chunk_headers(data, footer_start, ps_start, &mut headers);
    }
    for s in metadata.stripe_metadatas() {
        let offset = s.offset() as usize;
        let index_end = offset + s.index_length() as usize;
        let data_end = index_end + s.data_length() as usize;
        let footer_end = data_end + s.footer_length() as usize;
        regions.push(Region {
            name: "stripe_index",
            start: offset,
            end: index_end,
        });
        regions.push(Region {
            name: "stripe_data",
            start: index_end,
            end: data_end,
        });
        regions.push(Region {
            name: "stripe_footer",
            start: data_end,
            end: footer_end,
        });
        if compressed {
            chunk_headers(data, offset, footer_end, &mut headers);
        }
    }
    regions.retain(|r| r.end > r.start);
    (regions, headers)
}

fn mutate(data: &[u8], rng: &mut Rng) -> (Vec<u8>, String) {
    let (regions, headers) = regions(data);
    let region = &regions[rng.below(regions.len())];
    let mut out = data.to_vec();
    let pick = |rng: &mut Rng| region.start + rng.below(region.end - region.start);
    let desc;
    match rng.below(7) {
        0 => {
            let n = 1 + rng.below(3);
            for _ in 0..n {
                let p = pick(rng);
                out[p] ^= 1 << rng.below(8);
            }
            desc = format!("bitflip x{} in {}", n, region.name);
        }
        1 => {
            let p = pick(rng);
            let v = [0x00, 0xFF, 0x7F, 0x80, 0x01][rng.below(5)];
            let v = if rng.below(4) == 0 {
                rng.next() as u8
            } else {
                v
            };
            out[p] = v;
            desc = format!("set {:#04x} in {}", v, region.name);
        }
        2 => {
            let p = if rng.below(2) == 0 {
                rng.below(data.len())
            } else {
                region.start + rng.below(region.end - region.start)
            };
            out.truncate(p);
            desc = format!("truncate at {} (region {})", p, region.name);
        }
        3 => {
            let p = pick(rng);
            let n = (1 + rng.below(64)).min(region.end - p);
            let chunk = data[p..p + n].to_vec();
            let at = pick(rng);
            out.splice(at..at, chunk);
            desc = format!("duplicate {} bytes in {}", n, region.name);
        }
        4 => {
            let p = pick(rng);
            let n = (1 + rng.below(64)).min(region.end - p);
            out.drain(p..p + n);
            desc = format!("delete {} bytes in {}", n, region.name);
        }
        _ => {
            if headers.is_empty() {
                let p = pick(rng);
                out[p] ^= 1 << rng.below(8);
                desc = format!("bitflip in {} (uncompressed)", region.name);
            } else {
                let p = headers[rng.below(headers.len())];
                let raw = data[p] as u32 | (data[p + 1] as u32) << 8 | (data[p + 2] as u32) << 16;
                let (len, orig) = (raw >> 1, raw & 1);
                let (len, orig) = match rng.below(5) {
                    0 => (0, orig),
                    1 => (len.wrapping_add(1), orig),
                    2 => (len.wrapping_sub(1), orig),
                    3 => ([0x7F_FFFF, 0x10000, 0x4_0000, 0xFFFF][rng.below(4)], orig),
                    _ => (len, orig ^ 1),
                };
                let raw = ((len & 0x7F_FFFF) << 1) | orig;
                out[p] = raw as u8;
                out[p + 1] = (raw >> 8) as u8;
                out[p + 2] = (raw >> 16) as u8;
                desc = format!("chunk header at {} len={} original={}", p, len, orig);
            }
        }
    }
    (out, desc)
}

static PANIC: Mutex<String> = Mutex::new(String::new());

fn read_all(data: &[u8], batch_size: usize, column: Option<usize>) -> Result<u64, String> {
    let builder = ArrowReaderBuilder::try_new(Bytes::copy_from_slice(data))
        .map_err(|e| e.to_string())?
        .with_batch_size(batch_size);
    let builder = match column {
        Some(c) => {
            let mask = ProjectionMask::roots(builder.file_metadata().root_data_type(), [c]);
            builder.with_projection(mask)
        }
        None => builder,
    };
    let mut hasher = DefaultHasher::new();
    for batch in builder.build() {
        let batch = batch.map_err(|e| e.to_string())?;
        format!("{:?}", batch.columns()).hash(&mut hasher);
    }
    Ok(hasher.finish())
}

fn column_indices(data: &[u8]) -> Vec<usize> {
    let mut reader = Bytes::copy_from_slice(data);
    match read_metadata(&mut reader) {
        Ok(m) => m
            .root_data_type()
            .children()
            .iter()
            .map(|c| c.data_type().column_index())
            .collect(),
        Err(_) => vec![],
    }
}

fn run_reads(seed: &[u8], mutant: &[u8], rng: &mut Rng) -> (String, String) {
    let cols = column_indices(seed);
    let column = if cols.is_empty() {
        None
    } else {
        Some(cols[rng.below(cols.len())])
    };
    let mut status = "ok".to_string();
    let mut detail = String::new();
    let mut hashes = vec![];
    for (bs, col) in [(1, None), (8192, None), (1, column), (8192, column)] {
        let result = catch_unwind(AssertUnwindSafe(|| read_all(mutant, bs, col)));
        match result {
            Ok(Ok(h)) => hashes.push(h),
            Ok(Err(e)) => {
                if status == "ok" {
                    status = "err".into();
                    detail = e;
                }
            }
            Err(_) => {
                return ("panic".into(), PANIC.lock().unwrap().clone());
            }
        }
    }
    if status == "ok" {
        let orig = read_all(seed, 8192, None).ok();
        if hashes.get(1).copied() != orig {
            status = "diff".into();
        }
    }
    (status, detail)
}

fn worker(seed_dir: &Path, out_dir: &Path, rng_seed: u64, start: usize, count: usize) {
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            String::new()
        };
        let backtrace = std::backtrace::Backtrace::force_capture().to_string();
        let frame = backtrace
            .lines()
            .map(str::trim)
            .find(|l| l.contains("orc_rust::") && !l.contains("fuzz_mutate"))
            .unwrap_or("");
        *PANIC.lock().unwrap() = format!("{} @ {} in {}", msg.replace('\n', " "), loc, frame);
    }));
    let seeds = seed_files(seed_dir);
    let seed_data: Vec<Vec<u8>> = seeds.iter().map(|p| std::fs::read(p).unwrap()).collect();
    let stdout = std::io::stdout();
    for i in start..start + count {
        let mut rng = Rng(rng_seed ^ (i as u64).wrapping_mul(0x2545_F491_4F6C_DD1D));
        let s = i % seeds.len();
        let (mutant, desc) = mutate(&seed_data[s], &mut rng);
        std::fs::write(out_dir.join("current.orc"), &mutant).unwrap();
        let name = seeds[s].file_name().unwrap().to_string_lossy().into_owned();
        writeln!(stdout.lock(), "S {} {} | {}", i, name, desc).unwrap();
        stdout.lock().flush().unwrap();
        let (status, detail) = run_reads(&seed_data[s], &mutant, &mut rng);
        if status == "panic" {
            let _ = std::fs::copy(
                out_dir.join("current.orc"),
                out_dir.join(format!("panic_{rng_seed}_{i}.orc")),
            );
        }
        writeln!(
            stdout.lock(),
            "R {} {} {}",
            i,
            status,
            detail.replace('\n', " ")
        )
        .unwrap();
        stdout.lock().flush().unwrap();
    }
}

fn seed_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "orc"))
        .collect();
    v.sort();
    v
}

type Signatures = BTreeMap<String, (usize, usize, String)>;

fn driver(seed_dir: &Path, out_dir: &Path, rng_seed: u64, count: usize) {
    std::fs::create_dir_all(out_dir).unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut signatures = Signatures::new();
    let mut next = 0;
    while next < count {
        let script = format!(
            "ulimit -v {MEMORY_LIMIT_KB}; exec \"$0\" worker \"$@\" 2>{}",
            out_dir.join("worker.stderr").display()
        );
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(script)
            .arg(&exe)
            .arg(seed_dir)
            .arg(out_dir)
            .arg(rng_seed.to_string())
            .arg(next.to_string())
            .arg((count - next).to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let (tx, rx) = mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let mut current: Option<(usize, String)> = None;
        let mut finished = false;
        loop {
            match rx.recv_timeout(HANG_TIMEOUT) {
                Ok(line) => {
                    if let Some(rest) = line.strip_prefix("S ") {
                        let id: usize = rest.split(' ').next().unwrap().parse().unwrap();
                        current = Some((id, rest.to_string()));
                    } else if let Some(rest) = line.strip_prefix("R ") {
                        let mut parts = rest.splitn(3, ' ');
                        let id: usize = parts.next().unwrap().parse().unwrap();
                        let status = parts.next().unwrap().to_string();
                        let detail = parts.next().unwrap_or("").to_string();
                        *counts.entry(status.clone()).or_default() += 1;
                        if status == "panic" || status == "diff" {
                            let desc = current.as_ref().map(|c| c.1.clone()).unwrap_or_default();
                            let key = if status == "panic" {
                                format!("panic: {detail}")
                            } else {
                                format!("diff: {}", desc.split(" | ").nth(1).unwrap_or(""))
                            };
                            signatures.entry(key).or_insert((0, id, desc)).0 += 1;
                        }
                        next = id + 1;
                        current = None;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    classify(
                        &mut counts,
                        &mut signatures,
                        &current,
                        "hang",
                        out_dir,
                        rng_seed,
                    );
                    next = current.as_ref().map(|c| c.0 + 1).unwrap_or(next + 1);
                    break;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let status = child.wait().unwrap();
                    if current.is_none() && status.success() {
                        finished = true;
                        break;
                    }
                    let stderr =
                        std::fs::read_to_string(out_dir.join("worker.stderr")).unwrap_or_default();
                    let kind = if stderr.contains("memory allocation") {
                        "oom"
                    } else {
                        "abort"
                    };
                    classify(
                        &mut counts,
                        &mut signatures,
                        &current,
                        kind,
                        out_dir,
                        rng_seed,
                    );
                    next = current.as_ref().map(|c| c.0 + 1).unwrap_or(next + 1);
                    break;
                }
            }
        }
        if finished {
            break;
        }
    }
    println!("rng_seed={rng_seed} mutants={count}");
    for (k, v) in &counts {
        println!("{k}: {v}");
    }
    for (sig, (n, first, desc)) in &signatures {
        println!("SIG x{n} first={first} [{desc}] {sig}");
    }
}

fn classify(
    counts: &mut BTreeMap<String, usize>,
    signatures: &mut Signatures,
    current: &Option<(usize, String)>,
    kind: &str,
    out_dir: &Path,
    rng_seed: u64,
) {
    *counts.entry(kind.to_string()).or_default() += 1;
    if let Some((id, desc)) = current {
        let _ = std::fs::copy(
            out_dir.join("current.orc"),
            out_dir.join(format!("{kind}_{rng_seed}_{id}.orc")),
        );
        let key = format!("{kind}: see {kind}_{rng_seed}_{id}.orc");
        signatures.entry(key).or_insert((1, *id, desc.clone()));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("worker") {
        worker(
            Path::new(&args[2]),
            Path::new(&args[3]),
            args[4].parse().unwrap(),
            args[5].parse().unwrap(),
            args[6].parse().unwrap(),
        );
    } else if args.get(1).map(String::as_str) == Some("hash") {
        for path in &args[2..] {
            let data = std::fs::read(path).unwrap();
            println!("{path} {:?}", read_all(&data, 8192, None));
        }
    } else {
        driver(
            Path::new(&args[1]),
            Path::new(&args[2]),
            args[3].parse().unwrap(),
            args[4].parse().unwrap(),
        );
    }
}
