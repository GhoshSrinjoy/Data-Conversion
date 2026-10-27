//! Conversion orchestration.
//!
//! CSV input flows through: sniff → sample inference → waves of record-aligned
//! chunks decoded in parallel → sink.  The wave size comes from the memory budget, so
//! peak memory stays bounded however large the input is.  If a value later in the
//! file does not fit the inferred type, or the file is not valid UTF-8 after all, the
//! job re-plans (full-scan inference or an encoding fallback) and restarts; output
//! always goes to a temporary file first, so nothing partial is ever left behind.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::chunk::{Chunk, Chunker};
use crate::csvout::CsvWriteOptions;
use crate::decode::{ColumnBuilder, Violation};
use crate::error::{Error, Result};
use crate::io::{self, AtomicFile, Compression};
use crate::resources::{self, Plan};
use crate::sniff::{self, CsvDialect, SniffOptions};
use crate::tables::{self, IpcSink, JsonlSink, ParquetOptions, ParquetSink, Sink, Source};
use crate::tokenize::{malformed_value, unescape, Dialect, Tokenizer};
use crate::types::{ColumnStats, LogicalType, TypesMode};

/// Phase timings on stderr when DATACONV_TRACE is set (diagnosing slow jobs).
pub(crate) fn trace(phase: &str, started: Instant) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| std::env::var_os("DATACONV_TRACE").is_some()) {
        eprintln!("[dataconv] {phase:<24} {:>8.1} ms", started.elapsed().as_secs_f64() * 1000.0);
    }
}

/// Callbacks from a running job.  `progress` returns false to cancel.
pub trait Hooks: Sync {
    fn progress(&self, _done: u64, _total: u64) -> bool {
        true
    }
}

pub struct NoHooks;
impl Hooks for NoHooks {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BadLines {
    Error,
    Skip,
    #[default]
    Quarantine,
    /// Pad short rows with nulls; quarantine long ones.
    Pad,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InferMode {
    #[default]
    Sample,
    Full,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ConvertOptions {
    /// Source / destination format overrides (csv, tsv, jsonl, parquet, ipc, dcz).
    pub from: Option<String>,
    pub to: Option<String>,
    pub sniff: SniffOptions,
    pub types: TypesMode,
    /// Column name -> type override (see `LogicalType::parse`).
    pub schema: BTreeMap<String, String>,
    pub null_values: Vec<String>,
    pub bad_lines: BadLines,
    pub quarantine: Option<String>,
    pub infer: InferMode,
    pub sample_bytes: Option<usize>,
    pub threads: Option<usize>,
    pub memory: Option<u64>,
    pub chunk_size: Option<usize>,
    pub parquet: ParquetOptions,
    /// Output compression for text/IPC outputs and the default Parquet codec.
    pub compression: Option<String>,
    pub level: Option<i32>,
    pub profile: Option<String>,
    pub out_delimiter: Option<String>,
    pub out_header: Option<bool>,
    pub out_crlf: bool,
    pub overwrite: bool,
    pub columns: Option<Vec<String>>,
    pub limit: Option<u64>,
    /// Split into parts of at most this many rows / bytes (the destination is a directory).
    pub split_rows: Option<u64>,
    pub split_bytes: Option<u64>,
    /// Part name pattern, `{n}` or `{n:05}` is the part number (default `part-{n:05}`).
    pub split_pattern: Option<String>,
    /// Part file extension including the dot (default from the format).
    pub split_extension: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub source: String,
    pub destination: String,
    pub source_format: String,
    pub destination_format: String,
    pub engine: &'static str,
    pub rows: u64,
    pub columns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub seconds: f64,
    pub threads: usize,
    pub memory_budget: u64,
    pub chunk_size: usize,
    pub schema: Vec<(String, String)>,
    pub compression: String,
    pub dialect: Option<CsvDialect>,
    pub bad_rows: u64,
    pub blank_lines: u64,
    pub irregular_records: u64,
    pub quarantine: Option<String>,
    pub restarts: u32,
    pub warnings: Vec<String>,
    pub parts: Vec<crate::split::Part>,
}

// ---------------------------------------------------------------------------------
// Formats
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Csv,
    Jsonl,
    Parquet,
    Ipc,
    Dcz,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::Csv => "csv",
            Format::Jsonl => "jsonl",
            Format::Parquet => "parquet",
            Format::Ipc => "ipc",
            Format::Dcz => "dcz",
        }
    }

    pub fn parse(name: &str) -> Result<Format> {
        Ok(match name.trim_start_matches('.').to_ascii_lowercase().as_str() {
            "csv" | "tsv" | "tab" | "txt" | "psv" | "dat" => Format::Csv,
            "jsonl" | "ndjson" | "jsonlines" | "ldjson" => Format::Jsonl,
            "parquet" | "pq" | "parq" => Format::Parquet,
            "ipc" | "arrow" | "feather" | "fea" | "arrows" => Format::Ipc,
            "dcz" => Format::Dcz,
            other => return Err(Error::Format(format!("format '{other}' is not handled by the native engine"))),
        })
    }
}

fn inner_path(path: &Path) -> (PathBuf, Compression) {
    let comp = Compression::from_extension(path);
    if comp == Compression::None {
        (path.to_path_buf(), comp)
    } else {
        (path.with_extension(""), comp)
    }
}

/// Format and compression wrapper of an existing file (extension first, then magic).
pub fn detect_format(path: &Path, explicit: Option<&str>) -> Result<(Format, Compression)> {
    let head = io::read_head(path, 16)?;
    let comp = io::detect_compression(path, &head);
    if let Some(f) = explicit {
        return Ok((Format::parse(f)?, comp));
    }
    if head.starts_with(b"PAR1") {
        return Ok((Format::Parquet, Compression::None));
    }
    if head.starts_with(b"ARROW1") {
        return Ok((Format::Ipc, Compression::None));
    }
    if head.starts_with(crate::dcz::MAGIC) {
        return Ok((Format::Dcz, Compression::None));
    }
    let (inner, _) = inner_path(path);
    let ext = inner.extension().and_then(|e| e.to_str()).unwrap_or("");
    match Format::parse(ext) {
        Ok(f) => Ok((f, comp)),
        Err(_) if ext.eq_ignore_ascii_case("json") => {
            Err(Error::Format("JSON arrays are handled by the Python engine; use .jsonl for JSON Lines".into()))
        }
        Err(_) => Ok((Format::Csv, comp)),
    }
}

/// Format and compression wrapper implied by a destination path.
pub fn target_format(path: &Path, explicit: Option<&str>) -> Result<(Format, Compression)> {
    let (inner, comp) = inner_path(path);
    if let Some(f) = explicit {
        return Ok((Format::parse(f)?, comp));
    }
    let ext = inner.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext.is_empty() {
        return Err(Error::Invalid(format!("cannot tell the output format of {}; pass `to`", path.display())));
    }
    Ok((Format::parse(ext)?, comp))
}

// ---------------------------------------------------------------------------------
// CSV input
// ---------------------------------------------------------------------------------

/// A decoded UTF-8 stream of a CSV file (decompressed, transcoded, BOM removed).
fn open_csv_stream(path: &Path, encoding: &str) -> Result<(Box<dyn Read + Send>, Arc<AtomicU64>, u64)> {
    let (raw, consumed, size): (Box<dyn Read + Send>, Arc<AtomicU64>, u64) =
        if detect_format(path, None)?.0 == Format::Dcz {
            let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            let reader = crate::dcz::DczReader::open(path)?;
            let consumed = reader.consumed_counter();
            (Box::new(reader), consumed, size)
        } else {
            let input = io::open_input(path)?;
            (input.reader, input.consumed, input.file_size)
        };
    let reader: Box<dyn Read + Send> = match encoding_rs::Encoding::for_label(encoding.as_bytes()) {
        Some(enc) if enc != encoding_rs::UTF_8 => Box::new(io::Utf8Reader::new(raw, enc)),
        _ => Box::new(io::SkipBom::new(raw)),
    };
    Ok((reader, consumed, size))
}

pub fn sniff_file(path: &Path, opts: &SniffOptions) -> Result<CsvDialect> {
    let raw: Box<dyn Read + Send> = if detect_format(path, None)?.0 == Format::Dcz {
        Box::new(crate::dcz::DczReader::open(path)?)
    } else {
        io::open_input(path)?.reader
    };
    const SNIFF: usize = 2 << 20;
    let mut raw = raw;
    let head = io::read_up_to(&mut raw, SNIFF)?;
    let complete = head.len() < SNIFF;
    let mut o = opts.clone();
    if o.extension.is_none() {
        let (inner, _) = inner_path(path);
        o.extension = inner.extension().and_then(|e| e.to_str()).map(str::to_string);
    }
    sniff::sniff(&head, complete, &o)
}

#[derive(Clone)]
struct CsvPlan {
    dialect: Dialect,
    split: u8,
    leading: usize,
    ncols: usize,
    /// Source column index for every output column.
    projection: Vec<usize>,
    names: Vec<String>,
    types: Vec<LogicalType>,
    mode: TypesMode,
    extra_nulls: Vec<Vec<u8>>,
    bad_lines: BadLines,
    avg_record: usize,
}

impl CsvPlan {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.names.iter().zip(&self.types).map(|(n, t)| Field::new(n, t.arrow(), true)).collect::<Vec<_>>(),
        ))
    }
}

#[derive(Default)]
struct Decoded {
    batch: Option<RecordBatch>,
    rows: usize,
    bad: Vec<Vec<u8>>,
    blank: u64,
    irregular: u64,
    violation: Option<Violation>,
    error: Option<String>,
}

fn decode_chunk(plan: &CsvPlan, schema: &SchemaRef, chunk: &Chunk) -> Result<Decoded> {
    if simdutf8::basic::from_utf8(&chunk.data).is_err() {
        return Err(Error::Data("input is not valid UTF-8".into()));
    }
    let est = chunk.data.len() / plan.avg_record.max(8) + 16;
    let mut builders: Vec<ColumnBuilder> = plan.types.iter().map(|t| ColumnBuilder::new(t, est, plan.mode)).collect();
    let mut out = Decoded::default();
    let mut skip = if chunk.index == 0 { plan.leading } else { 0 };
    let mut tok = Tokenizer::new(plan.dialect);
    let mut scratch = Vec::new();
    let d = plan.dialect;
    tok.for_each(&chunk.data, |rec| {
        if skip > 0 {
            skip -= 1;
            return true;
        }
        if rec.is_blank() {
            out.blank += 1;
            return true;
        }
        if !rec.regular {
            out.irregular += 1;
        }
        let nf = rec.fields.len();
        if nf != plan.ncols {
            let pad = plan.bad_lines == BadLines::Pad && nf < plan.ncols;
            if !pad {
                if plan.bad_lines == BadLines::Error {
                    out.error = Some(format!(
                        "record with {nf} fields where {} were expected: {}",
                        plan.ncols,
                        String::from_utf8_lossy(&rec.raw()[..rec.raw().len().min(200)])
                    ));
                    return false;
                }
                out.bad.push(rec.raw().to_vec());
                return true;
            }
        }
        for (out_idx, &src) in plan.projection.iter().enumerate() {
            let ok = if src < nf {
                let f = &rec.fields[src];
                if f.malformed {
                    let owned = malformed_value(rec.data, f, &d);
                    builders[out_idx].push(&owned, false, &plan.extra_nulls)
                } else {
                    let missing = !f.quoted && f.start == f.end;
                    let v = unescape(&rec.data[f.start..f.end], f, &d, &mut scratch);
                    builders[out_idx].push(v, missing, &plan.extra_nulls)
                }
            } else {
                builders[out_idx].push(b"", true, &plan.extra_nulls)
            };
            if !ok {
                let f = rec.fields.get(src);
                let value =
                    f.map(|f| String::from_utf8_lossy(&rec.data[f.start..f.end]).into_owned()).unwrap_or_default();
                out.violation = Some(Violation { column: out_idx, row: out.rows, value });
                return false;
            }
        }
        out.rows += 1;
        true
    });
    if out.violation.is_some() || out.error.is_some() {
        return Ok(out);
    }
    if out.rows > 0 {
        let arrays = builders.into_iter().map(|b| b.finish()).collect();
        out.batch = Some(RecordBatch::try_new(schema.clone(), arrays)?);
    }
    Ok(out)
}

/// Statistics of every column over a sample (or the whole stream).
fn observe_chunk(plan: &CsvPlan, data: &[u8], skip: usize, stats: &mut [ColumnStats]) -> (u64, u64) {
    let mut tok = Tokenizer::new(plan.dialect);
    let mut scratch = Vec::new();
    let mut skip = skip;
    let (mut good, mut bad) = (0u64, 0u64);
    let d = plan.dialect;
    tok.for_each(data, |rec| {
        if skip > 0 {
            skip -= 1;
            return true;
        }
        if rec.is_blank() {
            return true;
        }
        let nf = rec.fields.len();
        if nf != plan.ncols && !(plan.bad_lines == BadLines::Pad && nf < plan.ncols) {
            bad += 1;
            return true;
        }
        good += 1;
        for (out_idx, &src) in plan.projection.iter().enumerate() {
            if src >= nf {
                stats[out_idx].observe(b"", plan.mode, &plan.extra_nulls);
                continue;
            }
            let f = &rec.fields[src];
            if f.malformed {
                let owned = malformed_value(rec.data, f, &d);
                stats[out_idx].observe(&owned, plan.mode, &plan.extra_nulls);
            } else if !f.quoted && f.start == f.end {
                stats[out_idx].observe(b"", plan.mode, &plan.extra_nulls);
            } else {
                // A quoted `""` is missing in typed columns, exactly as the decoder treats it.
                let v = unescape(&rec.data[f.start..f.end], f, &d, &mut scratch);
                stats[out_idx].observe(v, plan.mode, &plan.extra_nulls);
            }
        }
        true
    });
    (good, bad)
}

/// Up to `n` record-aligned ranges covering `data` (which starts at a record boundary).
fn record_ranges(data: &[u8], quote: Option<u8>, term: u8, n: usize) -> Vec<std::ops::Range<usize>> {
    // One forward pass: quote parity is carried from cut to cut, so the whole split
    // costs O(len) rather than one scan per part.
    let count = |s: &[u8], q: u8| s.iter().filter(|&&b| b == q).count();
    let mut cuts = vec![0usize];
    let mut pos = 0usize;
    let mut parity = 0usize;
    for k in 1..n {
        let target = data.len() * k / n;
        if target <= pos {
            continue;
        }
        if let Some(q) = quote {
            parity += count(&data[pos..target], q);
        }
        pos = target;
        loop {
            let Some(i) = memchr::memchr(term, &data[pos..]) else {
                pos = data.len();
                break;
            };
            let nl = pos + i;
            if let Some(q) = quote {
                parity += count(&data[pos..nl], q);
            }
            pos = nl + 1;
            if parity.is_multiple_of(2) {
                cuts.push(pos);
                break;
            }
        }
        if pos >= data.len() {
            break;
        }
    }
    cuts.push(data.len());
    cuts.dedup();
    cuts.windows(2).filter(|w| w[1] > w[0]).map(|w| w[0]..w[1]).collect()
}

fn sample_stats(
    path: &Path,
    plan: &CsvPlan,
    encoding: &str,
    sample_bytes: usize,
    file_size: u64,
) -> Result<Vec<ColumnStats>> {
    let ncols = plan.projection.len();
    let mut stats = vec![ColumnStats::default(); ncols];
    let t = Instant::now();
    let (reader, _, _) = open_csv_stream(path, encoding)?;
    let mut chunker = Chunker::new(reader, sample_bytes, plan.dialect.quote, plan.split);
    let head = chunker.next_chunk()?;
    trace("  read head sample", t);
    let t = Instant::now();
    if let Some(head) = head {
        // Split the head sample at record boundaries and observe the parts in parallel.
        let parts = record_ranges(&head.data, plan.dialect.quote, plan.split, rayon::current_num_threads().max(1));
        let partials: Vec<Vec<ColumnStats>> = parts
            .par_iter()
            .enumerate()
            .map(|(i, r)| {
                let mut local = vec![ColumnStats::default(); ncols];
                observe_chunk(plan, &head.data[r.clone()], if i == 0 { plan.leading } else { 0 }, &mut local);
                local
            })
            .collect();
        for part in &partials {
            for (s, p) in stats.iter_mut().zip(part) {
                s.merge(p);
            }
        }
        trace(&format!("  observe head ({} parts)", partials.len()), t);
    }
    let t = Instant::now();
    // Spread samples across uncompressed UTF-8 files catch type changes late in the file.
    let plain = io::detect_compression(path, &io::read_head(path, 16)?) == Compression::None
        && detect_format(path, None)?.0 != Format::Dcz
        && encoding_rs::Encoding::for_label(encoding.as_bytes()) == Some(encoding_rs::UTF_8);
    if plain && file_size > (sample_bytes as u64) * 2 {
        use std::io::{Seek, SeekFrom};
        const WINDOWS: u64 = 16;
        // The spread windows together read about as much as the head sample.
        let window = (sample_bytes as u64 / WINDOWS).clamp(64 << 10, 1 << 20) as usize;
        let mut parts: Vec<Vec<ColumnStats>> = (1..=WINDOWS)
            .into_par_iter()
            .filter_map(|i| {
                let offset = file_size * i / (WINDOWS + 1);
                let mut f = File::open(path).ok()?;
                f.seek(SeekFrom::Start(offset)).ok()?;
                let buf = io::read_up_to(&mut f, window).ok()?;
                let start = memchr::memchr(plan.split, &buf)? + 1;
                let end = memchr::memrchr(plan.split, &buf)? + 1;
                if end <= start {
                    return None;
                }
                let slice = sniff::trim_partial_utf8(&buf[start..end]);
                if simdutf8::basic::from_utf8(slice).is_err() {
                    return None;
                }
                let mut local = vec![ColumnStats::default(); ncols];
                let (good, bad) = observe_chunk(plan, slice, 0, &mut local);
                // A window that started inside a quoted field parses as garbage: drop it.
                (good > 0 && bad * 10 <= good).then_some(local)
            })
            .collect();
        for part in parts.drain(..) {
            for (s, p) in stats.iter_mut().zip(&part) {
                s.merge(p);
            }
        }
        trace("  spread samples", t);
    }
    Ok(stats)
}

fn full_stats(
    path: &Path,
    plan: &CsvPlan,
    encoding: &str,
    rplan: &Plan,
    pool: &rayon::ThreadPool,
) -> Result<Vec<ColumnStats>> {
    let ncols = plan.projection.len();
    let (reader, _, _) = open_csv_stream(path, encoding)?;
    let rx = spawn_chunker(Chunker::new(reader, rplan.chunk_size, plan.dialect.quote, plan.split), rplan.wave_chunks);
    let mut stats = vec![ColumnStats::default(); ncols];
    loop {
        let wave = next_wave(&rx, rplan.wave_chunks)?;
        if wave.is_empty() {
            break;
        }
        let parts: Vec<Vec<ColumnStats>> = pool.install(|| {
            wave.par_iter()
                .map(|c| {
                    let mut local = vec![ColumnStats::default(); ncols];
                    observe_chunk(plan, &c.data, if c.index == 0 { plan.leading } else { 0 }, &mut local);
                    local
                })
                .collect()
        });
        for part in parts {
            for (s, p) in stats.iter_mut().zip(&part) {
                s.merge(p);
            }
        }
    }
    Ok(stats)
}

fn spawn_chunker<R: Read + Send + 'static>(mut chunker: Chunker<R>, depth: usize) -> Receiver<Result<Chunk>> {
    let (tx, rx) = sync_channel::<Result<Chunk>>(depth.max(1));
    std::thread::Builder::new()
        .name("dataconv-reader".into())
        .spawn(move || loop {
            match chunker.next_chunk() {
                Ok(Some(c)) => {
                    if tx.send(Ok(c)).is_err() {
                        return;
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            }
        })
        .expect("failed to start reader thread");
    rx
}

fn next_wave(rx: &Receiver<Result<Chunk>>, n: usize) -> Result<Vec<Chunk>> {
    let mut wave = Vec::with_capacity(n);
    while wave.len() < n {
        match rx.recv() {
            Ok(Ok(c)) => wave.push(c),
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        }
    }
    Ok(wave)
}

fn decide_types(
    stats: &[ColumnStats],
    names: &[String],
    opts: &ConvertOptions,
    warnings: &mut Vec<String>,
) -> Result<Vec<LogicalType>> {
    let mut types = Vec::with_capacity(stats.len());
    for (s, name) in stats.iter().zip(names) {
        if let Some(spec) = opts.schema.get(name) {
            let ty = LogicalType::parse(spec)
                .ok_or_else(|| Error::Schema(format!("invalid type '{spec}' for column '{name}'")))?;
            types.push(ty);
            continue;
        }
        let ty = if opts.types == TypesMode::String { LogicalType::String } else { s.decide() };
        match &ty {
            LogicalType::Decimal { scale: 0, .. } if s.maybe_int => {
                warnings.push(format!("column '{name}': integers exceed int64, stored exactly as {}", ty.label()))
            }
            LogicalType::Decimal { .. } => warnings
                .push(format!("column '{name}': more digits than float64 holds, stored exactly as {}", ty.label())),
            LogicalType::String if s.ts_tz_mixed && s.maybe_ts => {
                warnings.push(format!("column '{name}' kept as text: it mixes UTC offsets"))
            }
            LogicalType::String => {
                if let Some(note) = &s.note {
                    warnings.push(format!("column '{name}' kept as text: {note}"));
                }
            }
            _ => {}
        }
        types.push(ty);
    }
    for key in opts.schema.keys() {
        if !names.contains(key) {
            return Err(Error::Schema(format!("schema override for unknown column '{key}'")));
        }
    }
    Ok(types)
}

// ---------------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------------

/// Where a job writes: one file, or a directory of parts.
pub(crate) struct Target {
    pub format: Format,
    pub compression: Compression,
    pub dst: PathBuf,
    /// (rows per part, bytes per part, name pattern, extension)
    pub split: Option<(Option<u64>, Option<u64>, String, String)>,
}

#[allow(clippy::too_many_arguments)]
fn build_sink(
    file: BufWriter<File>,
    format: Format,
    compression: Compression,
    schema: &SchemaRef,
    first: Option<&RecordBatch>,
    opts: &ConvertOptions,
    plan: &Plan,
    pool: &Arc<rayon::ThreadPool>,
    row_group_rows: Option<usize>,
) -> Result<Box<dyn Sink>> {
    Ok(match format {
        Format::Parquet => {
            let popts = pool.install(|| effective_parquet_options(opts, first));
            let rows = row_group_rows.or(popts.row_group_rows).unwrap_or(plan.row_group_rows).max(1);
            let props = popts.properties(schema, rows)?;
            Box::new(ParquetSink::new(file, schema.clone(), props, rows, pool.clone())?)
        }
        Format::Ipc => Box::new(IpcSink::new(file, schema, opts.compression.as_deref())?),
        Format::Csv | Format::Jsonl => {
            let w = tables::compressed_writer(file, compression, opts.level, plan.threads)?;
            if format == Format::Jsonl {
                Box::new(JsonlSink::new(w))
            } else {
                let mut o = CsvWriteOptions::default();
                if let Some(d) = opts.out_delimiter.as_deref().and_then(sniff::parse_char_option) {
                    o.delimiter = d;
                }
                o.header = opts.out_header.unwrap_or(true);
                if opts.out_crlf {
                    o.terminator = b"\r\n".to_vec();
                }
                Box::new(tables::CsvSink::new(w, schema, o)?)
            }
        }
        Format::Dcz => {
            let profile = crate::codec::Profile::parse(opts.profile.as_deref().unwrap_or("balanced"))?;
            let mut o = CsvWriteOptions::default();
            if let Some(d) = opts.out_delimiter.as_deref().and_then(sniff::parse_char_option) {
                o.delimiter = d;
            }
            let mut enc = crate::dcz::DczWriter::new(
                file,
                crate::dcz::EncodeOptions { profile, threads: plan.threads, block_size: None },
            )?
            .with_dialect(Dialect { delimiter: o.delimiter, ..Dialect::default() });
            crate::csvout::write_header(
                &mut enc,
                &schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
                &o,
            )?;
            Box::new(tables::CsvSink::new(Box::new(enc), schema, CsvWriteOptions { header: false, ..o })?)
        }
    })
}

/// Parquet options for a job, refined by its first batch: per-column encoding trials
/// for the max profile, otherwise no dictionary for high-cardinality columns.  The
/// split size estimator uses the same options, so its measurements match.
pub(crate) fn effective_parquet_options(opts: &ConvertOptions, first: Option<&RecordBatch>) -> ParquetOptions {
    let mut popts = parquet_options(opts);
    if let Some(sample) = first {
        if popts.should_tune() {
            let sample = sample.slice(0, sample.num_rows().min(200_000));
            popts.encodings = tables::tune_encodings(&sample, &popts);
        } else if popts.dictionary.is_none() && popts.encodings.is_empty() {
            popts.plain_columns = tables::high_cardinality_columns(sample);
        }
    }
    popts
}

fn parquet_options(opts: &ConvertOptions) -> ParquetOptions {
    let mut popts = opts.parquet.clone();
    if popts.compression.is_none() {
        popts.compression = opts.compression.clone();
    }
    if popts.level.is_none() {
        popts.level = opts.level;
    }
    if popts.profile.is_none() {
        popts.profile = opts.profile.clone();
    }
    popts
}

fn describe(format: Format, compression: Compression, opts: &ConvertOptions) -> String {
    match format {
        Format::Parquet => format!(
            "parquet/{}",
            parquet_options(opts).codec().map(|c| tables::describe_codec(&c)).unwrap_or_else(|_| "?".into())
        ),
        Format::Ipc => format!("ipc/{}", opts.compression.as_deref().unwrap_or("zstd")),
        Format::Dcz => format!("dcz/{}", opts.profile.as_deref().unwrap_or("balanced")),
        Format::Csv | Format::Jsonl => match compression {
            Compression::None => format.name().to_string(),
            c => format!("{}/{}", format.name(), c.name()),
        },
    }
}

type Opener<'a> = Box<dyn FnMut(&Path, Option<&RecordBatch>, usize) -> Result<(AtomicFile, Box<dyn Sink>)> + 'a>;

/// The output of a job, opened lazily so the first batch can tune the writer.
struct Out<'a> {
    target: &'a Target,
    schema: SchemaRef,
    opts: &'a ConvertOptions,
    plan: &'a Plan,
    pool: &'a Arc<rayon::ThreadPool>,
    single: Option<(AtomicFile, Box<dyn Sink>)>,
    rolling: Option<crate::split::RollingSink<Opener<'a>>>,
}

impl<'a> Out<'a> {
    fn new(
        target: &'a Target,
        schema: SchemaRef,
        opts: &'a ConvertOptions,
        plan: &'a Plan,
        pool: &'a Arc<rayon::ThreadPool>,
    ) -> Self {
        Out { target, schema, opts, plan, pool, single: None, rolling: None }
    }

    fn open(&mut self, first: Option<&RecordBatch>) -> Result<()> {
        let t = self.target;
        match &t.split {
            None => {
                let mut file = AtomicFile::create(&t.dst, self.opts.overwrite)?;
                let sink = build_sink(
                    file.take_writer(),
                    t.format,
                    t.compression,
                    &self.schema,
                    first,
                    self.opts,
                    self.plan,
                    self.pool,
                    None,
                )?;
                self.single = Some((file, sink));
            }
            Some((rows, bytes, pattern, extension)) => {
                let spec = crate::split::SplitSpec {
                    rows: *rows,
                    bytes: *bytes,
                    dir: t.dst.clone(),
                    pattern: pattern.clone(),
                    extension: extension.clone(),
                    overwrite: self.opts.overwrite,
                };
                let (format, compression, schema, opts, plan, pool) =
                    (t.format, t.compression, self.schema.clone(), self.opts, self.plan, self.pool);
                let opener: Opener<'a> = Box::new(move |path: &Path, first: Option<&RecordBatch>, rows: usize| {
                    let mut file = AtomicFile::create(path, opts.overwrite)?;
                    let rg = Some(rows.clamp(1, plan.row_group_rows));
                    let sink =
                        build_sink(file.take_writer(), format, compression, &schema, first, opts, plan, pool, rg)?;
                    Ok((file, sink))
                });
                self.rolling = Some(crate::split::RollingSink::new(
                    spec,
                    format,
                    compression,
                    self.schema.clone(),
                    self.opts.clone(),
                    opener,
                )?);
            }
        }
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.single.is_none() && self.rolling.is_none() {
            self.open(Some(batch))?;
        }
        match (&mut self.single, &mut self.rolling) {
            (Some((_, sink)), _) => sink.write(batch),
            (None, Some(r)) => r.write(batch),
            _ => unreachable!(),
        }
    }

    fn finish(mut self) -> Result<(u64, Vec<crate::split::Part>)> {
        if self.single.is_none() && self.rolling.is_none() {
            self.open(None)?;
        }
        if let Some((file, sink)) = self.single.take() {
            sink.finish()?;
            return Ok((file.commit(None)?, Vec::new()));
        }
        let parts = self.rolling.take().expect("output opened").finish()?;
        Ok((parts.iter().map(|p| p.bytes).sum(), parts))
    }
}

// ---------------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------------

enum Retry {
    Violation(Violation),
    Encoding,
    Fatal(Error),
}

impl From<Error> for Retry {
    fn from(e: Error) -> Self {
        Retry::Fatal(e)
    }
}

pub fn convert(src: &Path, dst: &Path, opts: &ConvertOptions, hooks: &dyn Hooks) -> Result<Report> {
    let started = Instant::now();
    if !src.exists() {
        return Err(Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "no such file"), src));
    }
    if src.canonicalize().ok() == dst.canonicalize().ok() {
        return Err(Error::Invalid("source and destination are the same file".into()));
    }
    let (src_fmt, src_comp) = detect_format(src, opts.from.as_deref())?;
    let splitting = opts.split_rows.is_some() || opts.split_bytes.is_some();
    let bytes_in = std::fs::metadata(src).map(|m| m.len()).unwrap_or(0);
    if splitting {
        return split(src, src_fmt, src_comp, dst, opts, hooks, started, bytes_in);
    }
    let (dst_fmt, dst_comp) = target_format(dst, opts.to.as_deref())?;
    io::ensure_writable(dst, opts.overwrite)?;

    // Byte-exact paths: CSV <-> DCZ never re-encode values.
    if dst_fmt == Format::Dcz && src_fmt == Format::Csv {
        let profile = crate::codec::Profile::parse(opts.profile.as_deref().unwrap_or("balanced"))?;
        return crate::dcz::compress_file(
            src,
            dst,
            &crate::dcz::EncodeOptions { profile, threads: opts.threads.unwrap_or(0), block_size: opts.chunk_size },
            opts.overwrite,
            hooks,
        );
    }
    if src_fmt == Format::Dcz
        && dst_fmt == Format::Csv
        && opts.columns.is_none()
        && opts.limit.is_none()
        && dst_comp == Compression::None
        && opts.out_delimiter.is_none()
    {
        return crate::dcz::decompress_file(src, dst, opts.overwrite, opts.threads.unwrap_or(0), hooks);
    }

    let plan = resources::plan(Some(bytes_in), opts.threads, opts.memory, opts.chunk_size);
    let pool = Arc::new(resources::thread_pool(plan.threads));
    let mut report = Report {
        source: src.display().to_string(),
        destination: dst.display().to_string(),
        source_format: src_fmt.name().into(),
        destination_format: dst_fmt.name().into(),
        engine: "native",
        bytes_in,
        threads: plan.threads,
        memory_budget: plan.memory_budget,
        chunk_size: plan.chunk_size,
        ..Report::default()
    };

    let target = Target { format: dst_fmt, compression: dst_comp, dst: dst.to_path_buf(), split: None };
    match src_fmt {
        Format::Csv | Format::Dcz => convert_csv(src, &target, opts, &plan, &pool, hooks, &mut report)?,
        Format::Parquet | Format::Ipc | Format::Jsonl => {
            convert_table(src, src_fmt, &target, opts, &plan, &pool, hooks, &mut report)?
        }
    }
    report.seconds = started.elapsed().as_secs_f64();
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn split(
    src: &Path,
    src_fmt: Format,
    src_comp: Compression,
    dir: &Path,
    opts: &ConvertOptions,
    hooks: &dyn Hooks,
    started: Instant,
    bytes_in: u64,
) -> Result<Report> {
    if opts.split_rows == Some(0) || opts.split_bytes == Some(0) {
        return Err(Error::Invalid("split sizes must be positive".into()));
    }
    if dir.is_file() {
        return Err(Error::Invalid(format!("{} is a file; splitting writes into a directory", dir.display())));
    }
    let to = opts.to.clone().unwrap_or_else(|| src_fmt.name().to_string());
    let dst_fmt = Format::parse(&to)?;
    let extension = opts.split_extension.clone().unwrap_or_else(|| {
        match dst_fmt {
            Format::Csv => ".csv",
            Format::Jsonl => ".jsonl",
            Format::Parquet => ".parquet",
            Format::Ipc => ".arrow",
            Format::Dcz => ".dcz",
        }
        .to_string()
    });
    let dst_comp = Compression::from_extension(Path::new(&format!("x{extension}")));
    let pattern = opts.split_pattern.clone().unwrap_or_else(|| "part-{n:05}".to_string());
    let plan = resources::plan(Some(bytes_in), opts.threads, opts.memory, opts.chunk_size);
    let mut report = Report {
        source: src.display().to_string(),
        destination: dir.display().to_string(),
        source_format: src_fmt.name().into(),
        destination_format: dst_fmt.name().into(),
        engine: "native",
        bytes_in,
        threads: plan.threads,
        memory_budget: plan.memory_budget,
        chunk_size: plan.chunk_size,
        ..Report::default()
    };
    let byte_exact = src_fmt == Format::Csv
        && dst_fmt == Format::Csv
        && dst_comp == Compression::None
        && opts.columns.is_none()
        && opts.limit.is_none()
        && opts.out_delimiter.is_none()
        && opts.schema.is_empty()
        && src_comp == Compression::None;
    if byte_exact {
        let dialect = sniff_file(src, &opts.sniff)?;
        let spec = crate::split::SplitSpec {
            rows: opts.split_rows,
            bytes: opts.split_bytes,
            dir: dir.to_path_buf(),
            pattern,
            extension,
            overwrite: opts.overwrite,
        };
        let parts = crate::split::split_csv_bytes(src, &spec, &dialect, hooks)?;
        report.rows = parts.iter().map(|p| p.rows).sum();
        report.bytes_out = parts.iter().map(|p| p.bytes).sum();
        report.columns = dialect.columns.len();
        report.compression = "csv (byte-exact)".into();
        report.parts = parts;
        report.dialect = Some(dialect);
        report.seconds = started.elapsed().as_secs_f64();
        return Ok(report);
    }
    let pool = Arc::new(resources::thread_pool(plan.threads));
    let target = Target {
        format: dst_fmt,
        compression: dst_comp,
        dst: dir.to_path_buf(),
        split: Some((opts.split_rows, opts.split_bytes, pattern, extension)),
    };
    match src_fmt {
        Format::Csv | Format::Dcz => convert_csv(src, &target, opts, &plan, &pool, hooks, &mut report)?,
        Format::Parquet | Format::Ipc | Format::Jsonl => {
            convert_table(src, src_fmt, &target, opts, &plan, &pool, hooks, &mut report)?
        }
    }
    report.seconds = started.elapsed().as_secs_f64();
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn convert_csv(
    src: &Path,
    target: &Target,
    opts: &ConvertOptions,
    plan: &Plan,
    pool: &Arc<rayon::ThreadPool>,
    hooks: &dyn Hooks,
    report: &mut Report,
) -> Result<()> {
    let t = Instant::now();
    let mut dialect = sniff_file(src, &opts.sniff)?;
    trace("sniff", t);
    let projection: Vec<usize> = match &opts.columns {
        None => (0..dialect.columns.len()).collect(),
        Some(cols) => cols
            .iter()
            .map(|c| {
                dialect
                    .columns
                    .iter()
                    .position(|n| n == c)
                    .ok_or_else(|| Error::Schema(format!("no column named '{c}'")))
            })
            .collect::<Result<_>>()?,
    };
    let names: Vec<String> = projection.iter().map(|&i| dialect.columns[i].clone()).collect();
    let mode = if matches!(target.format, Format::Csv | Format::Dcz) && opts.schema.is_empty() {
        // Text to text: values pass through untouched.
        TypesMode::String
    } else {
        opts.types
    };
    let mut cplan = CsvPlan {
        dialect: dialect.tokenizer_dialect(),
        split: dialect.split_byte(),
        leading: dialect.leading_records(),
        ncols: dialect.columns.len(),
        projection,
        names: names.clone(),
        types: vec![LogicalType::String; names.len()],
        mode,
        extra_nulls: opts.null_values.iter().map(|s| s.as_bytes().to_vec()).collect(),
        bad_lines: opts.bad_lines,
        avg_record: (dialect.max_record_len / 2).max(16),
    };
    let sample_bytes = opts.sample_bytes.unwrap_or(16 << 20).max(64 << 10);
    let mut warnings = Vec::new();
    let mut full_done = opts.infer == InferMode::Full;
    if mode != TypesMode::String || !opts.schema.is_empty() {
        let t = Instant::now();
        let stats = if full_done {
            full_stats(src, &cplan, &dialect.encoding, plan, pool)?
        } else {
            pool.install(|| sample_stats(src, &cplan, &dialect.encoding, sample_bytes, report.bytes_in))?
        };
        cplan.types = decide_types(&stats, &names, opts, &mut warnings)?;
        trace("infer types", t);
    }

    let mut restarts = 0u32;
    loop {
        let t = Instant::now();
        let attempt = csv_attempt(src, target, &cplan, &dialect.encoding, opts, plan, pool, hooks, report);
        trace("convert", t);
        match attempt {
            Ok(()) => break,
            Err(Retry::Fatal(e)) => return Err(e),
            Err(Retry::Encoding) => {
                restarts += 1;
                if dialect.encoding != "utf-8" || restarts > 3 {
                    return Err(Error::Data("input is not valid in any supported encoding".into()));
                }
                warnings.push("input is not valid UTF-8; re-read as windows-1252".into());
                dialect.encoding = "windows-1252".into();
            }
            Err(Retry::Violation(v)) => {
                restarts += 1;
                let name = cplan.names[v.column].clone();
                if !full_done {
                    warnings.push(format!(
                        "column '{name}': value '{}' beyond the sample does not fit {}; re-planned from a full scan",
                        v.value,
                        cplan.types[v.column].label()
                    ));
                    let stats = full_stats(src, &cplan, &dialect.encoding, plan, pool)?;
                    let mut ignored = Vec::new();
                    cplan.types = decide_types(&stats, &names, opts, &mut ignored)?;
                    full_done = true;
                } else if opts.schema.contains_key(&name) {
                    return Err(Error::Schema(format!(
                        "column '{name}': value '{}' cannot be stored as {}",
                        v.value,
                        cplan.types[v.column].label()
                    )));
                } else {
                    warnings.push(format!("column '{name}': value '{}' forced the column to text", v.value));
                    cplan.types[v.column] = LogicalType::String;
                }
                if restarts as usize > cplan.names.len() + 4 {
                    return Err(Error::Data("could not settle on a schema".into()));
                }
            }
        }
    }
    report.restarts = restarts;
    report.warnings.extend(warnings);
    report.schema = cplan.names.iter().cloned().zip(cplan.types.iter().map(|t| t.label())).collect();
    report.columns = cplan.names.len();
    report.dialect = Some(dialect);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn csv_attempt(
    src: &Path,
    target: &Target,
    cplan: &CsvPlan,
    encoding: &str,
    opts: &ConvertOptions,
    plan: &Plan,
    pool: &Arc<rayon::ThreadPool>,
    hooks: &dyn Hooks,
    report: &mut Report,
) -> std::result::Result<(), Retry> {
    let started_attempt = Instant::now();
    let (reader, consumed, total) = open_csv_stream(src, encoding)?;
    let rx = spawn_chunker(Chunker::new(reader, plan.chunk_size, cplan.dialect.quote, cplan.split), plan.wave_chunks);
    let schema = cplan.schema();
    let dst = target.dst.as_path();
    let mut out = Out::new(target, schema.clone(), opts, plan, pool);
    // Rejected rows go to a temporary file too, published only if the job succeeds.
    let mut quarantine: Option<(PathBuf, AtomicFile)> = None;
    let (mut rows, mut bad, mut blank, mut irregular) = (0u64, 0u64, 0u64, 0u64);
    let limit = opts.limit.unwrap_or(u64::MAX);
    // Ordered streaming pipeline: chunks are decoded on worker threads as soon as
    // they are read, and written in file order as they complete, so reading,
    // decoding and encoding overlap.  At most `depth` chunks are in flight.
    let shared = Arc::new(cplan.clone());
    let (res_tx, res_rx) = std::sync::mpsc::channel::<(usize, Result<Decoded>)>();
    let depth = plan.wave_chunks.max(2);
    let mut pending: std::collections::BTreeMap<usize, Result<Decoded>> = std::collections::BTreeMap::new();
    let (mut spawned, mut emitted, mut in_flight) = (0usize, 0usize, 0usize);
    let mut input_done = false;
    'stream: loop {
        while !input_done && in_flight < depth {
            match rx.recv() {
                Ok(Ok(chunk)) => {
                    let (tx, p, s, idx) = (res_tx.clone(), shared.clone(), schema.clone(), spawned);
                    pool.spawn(move || {
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_chunk(&p, &s, &chunk)))
                            .unwrap_or_else(|_| Err(Error::Data("internal error while decoding a chunk".into())));
                        let _ = tx.send((idx, r));
                    });
                    spawned += 1;
                    in_flight += 1;
                }
                Ok(Err(Error::Data(m))) if m.contains("decoding failed") => return Err(Retry::Encoding),
                Ok(Err(e)) => return Err(Retry::Fatal(e)),
                Err(_) => input_done = true,
            }
        }
        if in_flight == 0 {
            break;
        }
        let (idx, result) = res_rx.recv().map_err(|_| Retry::Fatal(Error::Data("a worker thread stopped".into())))?;
        in_flight -= 1;
        pending.insert(idx, result);
        while let Some(d) = pending.remove(&emitted) {
            emitted += 1;
            let d = match d {
                Ok(d) => d,
                Err(Error::Data(m)) if m.contains("UTF-8") => return Err(Retry::Encoding),
                Err(e) => return Err(Retry::Fatal(e)),
            };
            if let Some(v) = d.violation {
                return Err(Retry::Violation(v));
            }
            if let Some(msg) = d.error {
                return Err(Retry::Fatal(Error::Data(msg)));
            }
            blank += d.blank;
            irregular += d.irregular;
            if !d.bad.is_empty() {
                bad += d.bad.len() as u64;
                if cplan.bad_lines != BadLines::Skip {
                    if quarantine.is_none() {
                        let path =
                            opts.quarantine.as_ref().map(PathBuf::from).unwrap_or_else(|| default_quarantine(dst));
                        let mut f = AtomicFile::create(&path, true)?;
                        crate::csvout::write_header(
                            f.writer(),
                            &cplan.names,
                            &CsvWriteOptions { delimiter: cplan.dialect.delimiter, ..CsvWriteOptions::default() },
                        )?;
                        quarantine = Some((path, f));
                    }
                    let w = quarantine.as_mut().unwrap().1.writer();
                    for raw in &d.bad {
                        w.write_all(raw).map_err(Error::from)?;
                        if !raw.ends_with(b"\n") && !raw.ends_with(b"\r") {
                            w.write_all(b"\n").map_err(Error::from)?;
                        }
                    }
                }
            }
            if let Some(mut batch) = d.batch {
                if rows + batch.num_rows() as u64 > limit {
                    batch = batch.slice(0, (limit - rows) as usize);
                }
                rows += batch.num_rows() as u64;
                out.write(&batch)?;
            }
            if rows >= limit {
                break 'stream;
            }
        }
        if !hooks.progress(consumed.load(Ordering::Relaxed), total) {
            return Err(Retry::Fatal(Error::Cancelled));
        }
    }
    drop(rx);
    trace("  decode + write", started_attempt);
    let t = Instant::now();
    let (bytes_out, parts) = out.finish()?;
    trace("  finish output", t);
    report.bytes_out = bytes_out;
    report.parts = parts;
    if let Some((path, f)) = quarantine {
        f.commit(None)?;
        report.quarantine = Some(path.display().to_string());
    }
    report.rows = rows;
    report.bad_rows = bad;
    report.blank_lines = blank;
    report.irregular_records = irregular;
    report.compression = describe(target.format, target.compression, opts);
    if bad > 0 {
        report.warnings.push(match cplan.bad_lines {
            BadLines::Skip => format!("{bad} malformed rows skipped"),
            _ => format!("{bad} malformed rows moved to {}", report.quarantine.as_deref().unwrap_or("?")),
        });
    }
    Ok(())
}

pub fn default_quarantine(dst: &Path) -> PathBuf {
    let name = dst.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "output".into());
    dst.with_file_name(format!("{name}.rejected.csv"))
}

#[allow(clippy::too_many_arguments)]
fn convert_table(
    src: &Path,
    src_fmt: Format,
    target: &Target,
    opts: &ConvertOptions,
    plan: &Plan,
    pool: &Arc<rayon::ThreadPool>,
    hooks: &dyn Hooks,
    report: &mut Report,
) -> Result<()> {
    let wave_bytes = plan.memory_budget / 5;
    let mut source: Box<dyn Source> = match src_fmt {
        Format::Parquet => {
            Box::new(tables::ParquetSource::open(src, wave_bytes, plan.threads, pool.clone(), opts.columns.as_deref())?)
        }
        Format::Ipc => Box::new(tables::IpcSource::open(src, plan.threads * 2, opts.columns.as_deref())?),
        Format::Jsonl => Box::new(tables::JsonlSource::open(
            src,
            plan.chunk_size,
            plan.wave_chunks,
            pool.clone(),
            opts.infer == InferMode::Full,
        )?),
        _ => unreachable!(),
    };
    let schema = source.schema();
    let mut out = Out::new(target, schema.clone(), opts, plan, pool);
    let mut rows = 0u64;
    let limit = opts.limit.unwrap_or(u64::MAX);
    while let Some(wave) = source.next_wave()? {
        for mut batch in wave {
            if rows >= limit {
                break;
            }
            if rows + batch.num_rows() as u64 > limit {
                batch = batch.slice(0, (limit - rows) as usize);
            }
            rows += batch.num_rows() as u64;
            out.write(&batch)?;
        }
        let (done, total) = source.progress();
        if !hooks.progress(done, total) {
            return Err(Error::Cancelled);
        }
        if rows >= limit {
            break;
        }
    }
    let (bytes_out, parts) = out.finish()?;
    report.bytes_out = bytes_out;
    report.parts = parts;
    report.rows = rows;
    report.columns = schema.fields().len();
    report.schema = schema.fields().iter().map(|f| (f.name().clone(), f.data_type().to_string())).collect();
    report.compression = describe(target.format, target.compression, opts);
    Ok(())
}
