//! Splitting one input into many outputs.
//!
//! * CSV to CSV is byte-exact: records are copied verbatim (no re-quoting) and every
//!   part starts with the original header line.
//! * Any other combination streams through the conversion pipeline into rolling
//!   outputs.  For size targets the rows per part come from a measured bytes-per-row
//!   estimate that is corrected after every finished part, so parts land close to
//!   the requested size whatever the format and compression.

use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use serde::Serialize;

use crate::chunk::Chunker;
use crate::convert::{Format, Hooks};
use crate::error::{Error, Result};
use crate::io::{self, AtomicFile, Compression};
use crate::tables::Sink;
use crate::tokenize::Tokenizer;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Part {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
}

/// `pattern` with `{n}` or `{n:0W}` replaced by the part number.
pub fn part_name(pattern: &str, n: usize) -> String {
    if let Some(start) = pattern.find("{n") {
        if let Some(end_rel) = pattern[start..].find('}') {
            let spec = &pattern[start + 2..start + end_rel];
            let width = spec.trim_start_matches(':').trim_start_matches('0').parse::<usize>().unwrap_or(0);
            let number = if spec.starts_with(":0") { format!("{n:0width$}") } else { format!("{n:width$}") };
            return format!("{}{}{}", &pattern[..start], number.trim(), &pattern[start + end_rel + 1..]);
        }
    }
    format!("{pattern}{n:05}")
}

pub struct SplitSpec {
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub dir: PathBuf,
    pub pattern: String,
    pub extension: String,
    pub overwrite: bool,
}

impl SplitSpec {
    fn path(&self, n: usize) -> PathBuf {
        self.dir.join(format!("{}{}", part_name(&self.pattern, n), self.extension))
    }
}

/// Byte-exact CSV split.  Returns the parts written.
pub fn split_csv_bytes(
    src: &Path,
    spec: &SplitSpec,
    dialect: &crate::sniff::CsvDialect,
    hooks: &dyn Hooks,
) -> Result<Vec<Part>> {
    let td = dialect.tokenizer_dialect();
    let input = io::open_input(src)?;
    let consumed = input.consumed.clone();
    let total = input.file_size;
    let mut chunker = Chunker::new(input.reader, 8 << 20, td.quote, dialect.split_byte());
    std::fs::create_dir_all(&spec.dir).map_err(|e| Error::io(e, &spec.dir))?;
    let mut header: Vec<u8> = Vec::new();
    let mut leading = dialect.leading_records();
    let mut parts: Vec<Part> = Vec::new();
    let mut current: Option<(AtomicFile, u64, u64)> = None;
    let mut tok = Tokenizer::new(td);
    let mut committed: Vec<PathBuf> = Vec::new();
    let result = (|| -> Result<()> {
        while let Some(chunk) = chunker.next_chunk()? {
            let data = &chunk.data;
            let mut pending: Vec<(usize, usize)> = Vec::new();
            tok.for_each(data, |r| {
                pending.push((r.start, r.end));
                true
            });
            for (start, end) in pending {
                let rec = &data[start..end];
                if leading > 0 {
                    leading -= 1;
                    header.extend_from_slice(rec);
                    continue;
                }
                if current.is_none() {
                    let path = spec.path(parts.len());
                    let mut f = AtomicFile::create(&path, spec.overwrite)?;
                    f.writer().write_all(&header)?;
                    current = Some((f, 0, header.len() as u64));
                }
                let (f, rows, bytes) = current.as_mut().unwrap();
                f.writer().write_all(rec)?;
                *rows += 1;
                *bytes += rec.len() as u64;
                let full = spec.rows.is_some_and(|n| *rows >= n) || spec.bytes.is_some_and(|b| *bytes >= b);
                if full {
                    let (f, rows, _) = current.take().unwrap();
                    let path = spec.path(parts.len());
                    let size = f.commit(None)?;
                    committed.push(path.clone());
                    parts.push(Part { path: path.display().to_string(), rows, bytes: size });
                }
            }
            if !hooks.progress(consumed.load(std::sync::atomic::Ordering::Relaxed), total) {
                return Err(Error::Cancelled);
            }
        }
        if let Some((f, rows, _)) = current.take() {
            let path = spec.path(parts.len());
            let size = f.commit(None)?;
            committed.push(path.clone());
            parts.push(Part { path: path.display().to_string(), rows, bytes: size });
        }
        Ok(())
    })();
    if let Err(e) = result {
        for p in committed {
            let _ = std::fs::remove_file(p);
        }
        return Err(e);
    }
    Ok(parts)
}

fn written_size(
    sample: &RecordBatch,
    format: Format,
    compression: Compression,
    opts: &crate::convert::ConvertOptions,
) -> usize {
    use crate::codec::{compress, CodecId};
    let rows = sample.num_rows().max(1);
    let compressed = |text: Vec<u8>| -> usize {
        let codec = match compression {
            Compression::None => return text.len(),
            Compression::Gzip | Compression::Zip => (CodecId::Zlib, opts.level.unwrap_or(6)),
            Compression::Zstd => (CodecId::Zstd, opts.level.unwrap_or(3)),
            Compression::Xz | Compression::Bzip2 => (CodecId::Lzma, opts.level.unwrap_or(6)),
            Compression::Lz4 => (CodecId::Lz4, 0),
            Compression::Brotli => (CodecId::Brotli, opts.level.unwrap_or(6)),
        };
        compress(codec.0, &text, codec.1).map(|c| c.len()).unwrap_or(text.len())
    };
    match format {
        Format::Parquet => {
            let mut buf = Vec::new();
            let popts = crate::convert::effective_parquet_options(opts, Some(sample));
            let props = popts.properties(&sample.schema(), rows).ok();
            parquet::arrow::ArrowWriter::try_new(&mut buf, sample.schema(), props)
                .and_then(|mut w| {
                    w.write(sample)?;
                    w.close()?;
                    Ok(())
                })
                .map(|_| buf.len())
                .unwrap_or(rows * 16)
        }
        Format::Ipc => sample.get_array_memory_size() / 2,
        Format::Jsonl => {
            let mut buf = Vec::new();
            {
                let mut w = arrow::json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow::json::writer::LineDelimited>(&mut buf);
                let _ = w.write(sample).and_then(|_| w.finish());
            }
            compressed(buf)
        }
        Format::Csv | Format::Dcz => {
            let mut buf = Vec::new();
            let _ = crate::csvout::write_batch(&mut buf, sample, &crate::csvout::CsvWriteOptions::default());
            if format == Format::Dcz {
                crate::dcz::compress_bytes(&buf, true, crate::codec::Profile::Balanced)
                    .map(|c| c.len())
                    .unwrap_or(buf.len())
            } else {
                compressed(buf)
            }
        }
    }
}

/// (fixed bytes per file, bytes per row) when `batch` is written as `format`,
/// measured by writing the sample at two sizes: dictionary pages, headers and footers
/// are fixed costs, so a single measurement would overstate the cost of a row.
pub fn measure_size_model(
    batch: &RecordBatch,
    format: Format,
    compression: Compression,
    opts: &crate::convert::ConvertOptions,
) -> (f64, f64) {
    let n = batch.num_rows().min(65_536);
    let full = batch.slice(0, n);
    let big = written_size(&full, format, compression, opts) as f64;
    if n < 2048 {
        return (0.0, (big / n.max(1) as f64).max(1.0));
    }
    let small = written_size(&batch.slice(0, n / 2), format, compression, opts) as f64;
    let per_row = ((big - small) / (n - n / 2) as f64).max(0.5);
    let fixed = (big - per_row * n as f64).max(0.0);
    (fixed, per_row)
}

/// A sink that rolls over to a new file when a part is full.
///
/// For size targets the first part's row count comes from a size model measured on
/// the first 64K rows; after every part the row count is rescaled by how far that
/// part landed from the target (damped), which converges in a part or two.
pub struct RollingSink<F>
where
    F: FnMut(&Path, Option<&RecordBatch>, usize) -> Result<(AtomicFile, Box<dyn Sink>)>,
{
    spec: SplitSpec,
    open: F,
    format: Format,
    compression: Compression,
    current: Option<(AtomicFile, Box<dyn Sink>, u64)>,
    pub parts: Vec<Part>,
    rows_target: u64,
    measured: bool,
    staged: Vec<RecordBatch>,
    staged_rows: usize,
    opts: crate::convert::ConvertOptions,
    schema: SchemaRef,
}

const MEASURE_ROWS: usize = 65_536;

impl<F> RollingSink<F>
where
    F: FnMut(&Path, Option<&RecordBatch>, usize) -> Result<(AtomicFile, Box<dyn Sink>)>,
{
    pub fn new(
        spec: SplitSpec,
        format: Format,
        compression: Compression,
        schema: SchemaRef,
        opts: crate::convert::ConvertOptions,
        open: F,
    ) -> Result<Self> {
        std::fs::create_dir_all(&spec.dir).map_err(|e| Error::io(e, &spec.dir))?;
        let rows_target = spec.rows.unwrap_or(u64::MAX);
        let measured = spec.bytes.is_none();
        Ok(RollingSink {
            spec,
            open,
            format,
            compression,
            current: None,
            parts: Vec::new(),
            rows_target,
            measured,
            staged: Vec::new(),
            staged_rows: 0,
            opts,
            schema,
        })
    }

    fn roll(&mut self, full: bool) -> Result<()> {
        if let Some((file, sink, rows)) = self.current.take() {
            sink.finish()?;
            let path = self.spec.path(self.parts.len());
            let bytes = file.commit(None)?;
            self.parts.push(Part { path: path.display().to_string(), rows, bytes });
            if let (Some(target), true) = (self.spec.bytes, full) {
                let ratio = (target as f64 / bytes.max(1) as f64).powf(0.9).clamp(0.25, 4.0);
                let estimate = ((rows as f64) * ratio).round().max(1.0) as u64;
                self.rows_target = self.spec.rows.map_or(estimate, |r| r.min(estimate));
            }
        }
        Ok(())
    }

    fn measure(&mut self) -> Result<()> {
        self.measured = true;
        if let (Some(target), false) = (self.spec.bytes, self.staged.is_empty()) {
            let sample = arrow::compute::concat_batches(&self.schema, &self.staged)?;
            let (fixed, per_row) = measure_size_model(&sample, self.format, self.compression, &self.opts);
            let estimate = ((target as f64 - fixed).max(per_row) / per_row).round().max(1.0) as u64;
            self.rows_target = self.spec.rows.map_or(estimate, |r| r.min(estimate));
        }
        for batch in std::mem::take(&mut self.staged) {
            self.push(&batch)?;
        }
        self.staged_rows = 0;
        Ok(())
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.measured {
            self.staged.push(batch.clone());
            self.staged_rows += batch.num_rows();
            if self.staged_rows >= MEASURE_ROWS {
                self.measure()?;
            }
            return Ok(());
        }
        self.push(batch)
    }

    fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        let total = batch.num_rows();
        while offset < total {
            if self.current.is_none() {
                let path = self.spec.path(self.parts.len());
                let rg = self.rows_target.min(usize::MAX as u64) as usize;
                let (file, sink) =
                    (self.open)(&path, Some(&batch.slice(offset, (total - offset).min(MEASURE_ROWS))), rg)?;
                self.current = Some((file, sink, 0));
            }
            let (_, sink, rows) = self.current.as_mut().unwrap();
            let room = (self.rows_target - *rows).max(1);
            let take = (room as usize).min(total - offset);
            sink.write(&batch.slice(offset, take))?;
            *rows += take as u64;
            offset += take;
            if *rows >= self.rows_target {
                self.roll(true)?;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<Part>> {
        if !self.measured {
            self.measure()?;
        }
        self.roll(false)?;
        Ok(self.parts)
    }
}
