//! Columnar sources and sinks: Parquet, Arrow IPC (Feather v2), JSON Lines and CSV.
//!
//! Sinks receive record batches in order.  The Parquet sink encodes the columns of a
//! row group in parallel (most writers do this on one thread), and can tune each
//! column's encoding by trial for the smallest output.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnChunk, ArrowRowGroupWriterFactory, ArrowWriterOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression as PqCompression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::ColumnPath;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::chunk::Chunker;
use crate::csvout::{self, CsvWriteOptions};
use crate::error::{Error, Result};
use crate::io::Compression;

// ---------------------------------------------------------------------------------
// Output compression wrappers (for .csv.gz, .jsonl.zst, ...)
// ---------------------------------------------------------------------------------

pub trait FinishWrite: Write + Send {
    fn finish_box(self: Box<Self>) -> std::io::Result<()>;
}

struct Plain(BufWriter<File>);
impl Write for Plain {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.write(b)
    }
    fn write_all(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.0.write_all(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
impl FinishWrite for Plain {
    fn finish_box(mut self: Box<Self>) -> std::io::Result<()> {
        self.0.flush()
    }
}

macro_rules! finishing {
    ($ty:ty, |$s:ident| $finish:expr) => {
        impl FinishWrite for $ty {
            fn finish_box(self: Box<Self>) -> std::io::Result<()> {
                let $s = *self;
                let mut inner = $finish?;
                inner.flush()
            }
        }
    };
}

finishing!(flate2::write::GzEncoder<BufWriter<File>>, |s| s.finish());
finishing!(zstd::stream::write::Encoder<'static, BufWriter<File>>, |s| s.finish());
finishing!(liblzma::write::XzEncoder<BufWriter<File>>, |s| s.finish());
finishing!(lz4_flex::frame::FrameEncoder<BufWriter<File>>, |s| s
    .finish()
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));

impl FinishWrite for brotli::CompressorWriter<BufWriter<File>> {
    fn finish_box(self: Box<Self>) -> std::io::Result<()> {
        let mut inner = self.into_inner();
        inner.flush()
    }
}

/// Wrap `w` in a compressor chosen by the output path's extension.
pub fn compressed_writer(w: BufWriter<File>, compression: Compression, level: Option<i32>, threads: usize) -> Result<Box<dyn FinishWrite>> {
    Ok(match compression {
        Compression::None => Box::new(Plain(w)),
        Compression::Gzip => Box::new(flate2::write::GzEncoder::new(
            w,
            flate2::Compression::new(level.unwrap_or(6).clamp(0, 9) as u32),
        )),
        Compression::Zstd => {
            let mut enc = zstd::stream::write::Encoder::new(w, level.unwrap_or(3))?;
            if threads > 1 {
                enc.multithread(threads.min(64) as u32)?;
            }
            Box::new(enc)
        }
        Compression::Xz => Box::new(liblzma::write::XzEncoder::new(w, level.unwrap_or(6).clamp(0, 9) as u32)),
        Compression::Lz4 => Box::new(lz4_flex::frame::FrameEncoder::new(w)),
        Compression::Brotli => Box::new(brotli::CompressorWriter::new(w, 1 << 16, level.unwrap_or(6).clamp(0, 11) as u32, 22)),
        Compression::Bzip2 | Compression::Zip => {
            return Err(Error::Format(format!("{} output is not supported by the native engine", compression.name())))
        }
    })
}

// ---------------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------------

pub trait Sink: Send {
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ParquetOptions {
    /// none | snappy | gzip | brotli | lz4 | zstd
    pub compression: Option<String>,
    pub level: Option<i32>,
    /// fast | balanced | max
    pub profile: Option<String>,
    pub row_group_rows: Option<usize>,
    pub dictionary: Option<bool>,
    /// column name -> encoding name (PLAIN, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT, ...)
    /// or "DICTIONARY"
    pub encodings: HashMap<String, String>,
    /// Tune each column's encoding by trial on the first batch (implied by profile=max).
    pub tune: Option<bool>,
    pub statistics: Option<bool>,
    /// "1.0" (most compatible, default) or "2.0"
    pub version: Option<String>,
}

pub fn parquet_compression(name: &str, level: Option<i32>) -> Result<PqCompression> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => PqCompression::UNCOMPRESSED,
        "snappy" => PqCompression::SNAPPY,
        "lz4" | "lz4_raw" => PqCompression::LZ4_RAW,
        "gzip" | "gz" => PqCompression::GZIP(GzipLevel::try_new(level.unwrap_or(6).clamp(0, 9) as u32)?),
        "brotli" | "br" => PqCompression::BROTLI(BrotliLevel::try_new(level.unwrap_or(6).clamp(0, 11) as u32)?),
        "zstd" | "zst" => PqCompression::ZSTD(ZstdLevel::try_new(level.unwrap_or(3).clamp(1, 22))?),
        other => return Err(Error::Invalid(format!("unknown Parquet compression '{other}'"))),
    })
}

pub fn describe_codec(c: &PqCompression) -> String {
    match c {
        PqCompression::UNCOMPRESSED => "none".into(),
        PqCompression::SNAPPY => "snappy".into(),
        PqCompression::LZ4_RAW | PqCompression::LZ4 => "lz4".into(),
        PqCompression::GZIP(l) => format!("gzip-{}", l.compression_level()),
        PqCompression::BROTLI(l) => format!("brotli-{}", l.compression_level()),
        PqCompression::ZSTD(l) => format!("zstd-{}", l.compression_level()),
        PqCompression::LZO => "lzo".into(),
    }
}

fn encoding_from_name(name: &str) -> Result<Option<Encoding>> {
    Ok(Some(match name.to_ascii_uppercase().as_str() {
        "DICTIONARY" | "DICT" | "RLE_DICTIONARY" => return Ok(None),
        "PLAIN" => Encoding::PLAIN,
        "DELTA_BINARY_PACKED" | "DELTA" => Encoding::DELTA_BINARY_PACKED,
        "DELTA_LENGTH_BYTE_ARRAY" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        "DELTA_BYTE_ARRAY" => Encoding::DELTA_BYTE_ARRAY,
        "BYTE_STREAM_SPLIT" | "BSS" => Encoding::BYTE_STREAM_SPLIT,
        "RLE" => Encoding::RLE,
        other => return Err(Error::Invalid(format!("unknown Parquet encoding '{other}'"))),
    }))
}

impl ParquetOptions {
    fn profile(&self) -> &str {
        self.profile.as_deref().unwrap_or("balanced")
    }

    pub fn codec(&self) -> Result<PqCompression> {
        let (name, level) = match (&self.compression, self.profile()) {
            (Some(c), _) if c != "auto" => (c.as_str(), self.level),
            (_, "fast") => ("zstd", Some(self.level.unwrap_or(1))),
            (_, "max") => ("zstd", Some(self.level.unwrap_or(19))),
            _ => ("zstd", Some(self.level.unwrap_or(3))),
        };
        parquet_compression(name, level)
    }

    pub fn should_tune(&self) -> bool {
        self.tune.unwrap_or(self.profile() == "max") && self.encodings.is_empty()
    }

    pub fn properties(&self, schema: &Schema, rows_hint: usize) -> Result<WriterProperties> {
        let codec = self.codec()?;
        let version = match self.version.as_deref() {
            Some("2.0") | Some("2") | Some("v2") => WriterVersion::PARQUET_2_0,
            _ => WriterVersion::PARQUET_1_0,
        };
        let mut b = WriterProperties::builder()
            .set_compression(codec)
            .set_writer_version(version)
            .set_created_by(format!("dataconv {}", crate::VERSION))
            .set_max_row_group_row_count(Some(rows_hint.max(1)))
            .set_statistics_enabled(if self.statistics.unwrap_or(true) {
                EnabledStatistics::Page
            } else {
                EnabledStatistics::None
            });
        if let Some(d) = self.dictionary {
            b = b.set_dictionary_enabled(d);
        }
        for (name, enc) in &self.encodings {
            if schema.field_with_name(name).is_err() {
                continue;
            }
            let path = ColumnPath::new(vec![name.clone()]);
            match encoding_from_name(enc)? {
                None => b = b.set_column_dictionary_enabled(path, true),
                Some(e) => {
                    b = b.set_column_dictionary_enabled(path.clone(), false).set_column_encoding(path, e);
                }
            }
        }
        Ok(b.build())
    }
}

fn candidates(dt: &DataType) -> &'static [&'static str] {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16
        | DataType::UInt32 | DataType::UInt64 | DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        | DataType::Time32(_) | DataType::Time64(_) => {
            &["DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT", "PLAIN"]
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            &["DICTIONARY", "BYTE_STREAM_SPLIT", "PLAIN"]
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            &["DICTIONARY", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN"]
        }
        _ => &[],
    }
}

/// Try each candidate encoding on a sample of every column and keep the smallest.
pub fn tune_encodings(sample: &RecordBatch, opts: &ParquetOptions) -> HashMap<String, String> {
    let schema = sample.schema();
    schema
        .fields()
        .par_iter()
        .enumerate()
        .filter_map(|(i, field)| {
            let cands = candidates(field.data_type());
            if cands.is_empty() || sample.num_rows() == 0 {
                return None;
            }
            let single = Arc::new(Schema::new(vec![field.as_ref().clone()]));
            let batch = RecordBatch::try_new(single.clone(), vec![sample.column(i).clone()]).ok()?;
            let mut best: Option<(usize, &str)> = None;
            for &cand in cands {
                let mut trial = opts.clone();
                trial.encodings = HashMap::from([(field.name().clone(), cand.to_string())]);
                let size = (|| -> Result<usize> {
                    let props = trial.properties(&single, batch.num_rows())?;
                    let mut buf = Vec::new();
                    let mut w = ArrowWriter::try_new(&mut buf, single.clone(), Some(props))?;
                    w.write(&batch)?;
                    w.close()?;
                    Ok(buf.len())
                })();
                if let Ok(size) = size {
                    if best.map_or(true, |(b, _)| size < b) {
                        best = Some((size, cand));
                    }
                }
            }
            best.map(|(_, enc)| (field.name().clone(), enc.to_string()))
        })
        .collect()
}

pub struct ParquetSink {
    writer: Option<SerializedFileWriter<BufWriter<File>>>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    row_group_rows: usize,
    row_groups: usize,
    pool: Arc<rayon::ThreadPool>,
}

impl ParquetSink {
    pub fn new(
        file: BufWriter<File>,
        schema: SchemaRef,
        props: WriterProperties,
        row_group_rows: usize,
        pool: Arc<rayon::ThreadPool>,
    ) -> Result<Self> {
        let options = ArrowWriterOptions::new().with_properties(props);
        let writer = ArrowWriter::try_new_with_options(file, schema.clone(), options)?;
        let (writer, factory) = writer.into_serialized_writer()?;
        Ok(ParquetSink {
            writer: Some(writer),
            factory,
            schema,
            pending: Vec::new(),
            pending_rows: 0,
            row_group_rows: row_group_rows.max(1),
            row_groups: 0,
            pool,
        })
    }

    fn flush_row_group(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let writers = self.factory.create_column_writers(self.row_groups)?;
        let mut jobs: Vec<(_, Vec<_>)> = writers.into_iter().map(|w| (w, Vec::new())).collect();
        for batch in self.pending.drain(..) {
            let mut leaf = 0;
            for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
                for l in compute_leaves(field, column)? {
                    jobs[leaf].1.push(l);
                    leaf += 1;
                }
            }
        }
        let chunks: Vec<ArrowColumnChunk> = self.pool.install(|| {
            jobs.into_par_iter()
                .map(|(mut w, leaves)| {
                    for l in &leaves {
                        w.write(l)?;
                    }
                    w.close()
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })?;
        let writer = self.writer.as_mut().expect("writer open");
        let mut rg = writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut rg)?;
        }
        rg.close()?;
        self.row_groups += 1;
        self.pending_rows = 0;
        Ok(())
    }
}

impl Sink for ParquetSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        let rows = batch.num_rows();
        while offset < rows {
            let room = self.row_group_rows - self.pending_rows;
            let take = room.min(rows - offset);
            self.pending.push(batch.slice(offset, take));
            self.pending_rows += take;
            offset += take;
            if self.pending_rows >= self.row_group_rows {
                self.flush_row_group()?;
            }
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.flush_row_group()?;
        let writer = self.writer.take().expect("writer open");
        let mut inner = writer.into_inner()?;
        inner.flush()?;
        Ok(())
    }
}

pub struct IpcSink {
    writer: arrow::ipc::writer::FileWriter<BufWriter<File>>,
}

impl IpcSink {
    pub fn new(file: BufWriter<File>, schema: &Schema, compression: Option<&str>) -> Result<Self> {
        use arrow::ipc::writer::IpcWriteOptions;
        use arrow::ipc::CompressionType;
        let codec = match compression.map(|c| c.to_ascii_lowercase()).as_deref() {
            None | Some("auto") | Some("zstd") => Some(CompressionType::ZSTD),
            Some("lz4") => Some(CompressionType::LZ4_FRAME),
            Some("none") | Some("uncompressed") => None,
            Some(other) => return Err(Error::Invalid(format!("Arrow IPC supports zstd, lz4 or none, not '{other}'"))),
        };
        let options = IpcWriteOptions::default().try_with_compression(codec)?;
        let writer = arrow::ipc::writer::FileWriter::try_new_with_options(file, schema, options)?;
        Ok(IpcSink { writer })
    }
}

impl Sink for IpcSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.writer.finish()?;
        let mut inner = self.writer.into_inner()?;
        inner.flush()?;
        Ok(())
    }
}

pub struct CsvSink {
    out: Option<Box<dyn FinishWrite>>,
    options: CsvWriteOptions,
}

impl CsvSink {
    pub fn new(mut out: Box<dyn FinishWrite>, schema: &Schema, options: CsvWriteOptions) -> Result<Self> {
        if options.header {
            let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            csvout::write_header(&mut out, &names, &options)?;
        }
        Ok(CsvSink { out: Some(out), options })
    }
}

impl Sink for CsvSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        csvout::write_batch(self.out.as_mut().expect("open"), batch, &self.options)
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.out.take().expect("open").finish_box()?;
        Ok(())
    }
}

pub struct JsonlSink {
    out: Option<Box<dyn FinishWrite>>,
}

impl JsonlSink {
    pub fn new(out: Box<dyn FinishWrite>) -> Self {
        JsonlSink { out: Some(out) }
    }
}

fn encode_jsonl(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut w = arrow::json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow::json::writer::LineDelimited>(&mut buf);
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

impl Sink for JsonlSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        const SLICE: usize = 8192;
        let rows = batch.num_rows();
        let parts: Vec<Vec<u8>> = (0..rows)
            .step_by(SLICE)
            .collect::<Vec<_>>()
            .par_iter()
            .map(|&s| encode_jsonl(&batch.slice(s, SLICE.min(rows - s))))
            .collect::<Result<_>>()?;
        let out = self.out.as_mut().expect("open");
        for p in parts {
            out.write_all(&p)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.out.take().expect("open").finish_box()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------------

pub trait Source: Send {
    fn schema(&self) -> SchemaRef;
    /// The next batches in file order, or None when the input is exhausted.
    fn next_wave(&mut self) -> Result<Option<Vec<RecordBatch>>>;
    /// (bytes of the input file consumed, total bytes)
    fn progress(&self) -> (u64, u64);
    fn rows_hint(&self) -> Option<u64> {
        None
    }
}

pub struct ParquetSource {
    path: PathBuf,
    meta: ArrowReaderMetadata,
    next: usize,
    wave_bytes: u64,
    max_parallel: usize,
    batch_size: usize,
    done_bytes: u64,
    total_bytes: u64,
    pool: Arc<rayon::ThreadPool>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
}

impl ParquetSource {
    pub fn open(path: &Path, wave_bytes: u64, max_parallel: usize, pool: Arc<rayon::ThreadPool>, columns: Option<&[String]>) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::io(e, path))?;
        let total_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())?;
        let full = meta.schema().clone();
        let projection = match columns {
            None => None,
            Some(cols) => Some(
                cols.iter()
                    .map(|c| full.index_of(c).map_err(|_| Error::Schema(format!("no column named '{c}'"))))
                    .collect::<Result<Vec<_>>>()?,
            ),
        };
        let schema = match &projection {
            None => full,
            Some(p) => Arc::new(full.project(p)?),
        };
        Ok(ParquetSource {
            path: path.to_path_buf(),
            meta,
            next: 0,
            wave_bytes: wave_bytes.max(1),
            max_parallel: max_parallel.max(1),
            batch_size: 65_536,
            done_bytes: 0,
            total_bytes,
            pool,
            projection,
            schema,
        })
    }

    pub fn num_rows(&self) -> u64 {
        self.meta.metadata().file_metadata().num_rows().max(0) as u64
    }
}

impl Source for ParquetSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_wave(&mut self) -> Result<Option<Vec<RecordBatch>>> {
        let md = self.meta.metadata().clone();
        let n = md.num_row_groups();
        if self.next >= n {
            return Ok(None);
        }
        let mut groups = Vec::new();
        let mut bytes = 0u64;
        while self.next < n && groups.len() < self.max_parallel {
            let rg = md.row_group(self.next);
            let size = rg.total_byte_size().max(0) as u64;
            if !groups.is_empty() && bytes + size > self.wave_bytes {
                break;
            }
            bytes += size;
            self.done_bytes += rg.compressed_size().max(0) as u64;
            groups.push(self.next);
            self.next += 1;
        }
        let path = self.path.clone();
        let meta = self.meta.clone();
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let results: Vec<Vec<RecordBatch>> = self.pool.install(|| {
            groups
                .par_iter()
                .map(|&rg| -> Result<Vec<RecordBatch>> {
                    let file = File::open(&path).map_err(|e| Error::io(e, &path))?;
                    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta.clone())
                        .with_row_groups(vec![rg])
                        .with_batch_size(batch_size);
                    if let Some(p) = &projection {
                        let mask = parquet::arrow::ProjectionMask::roots(meta.parquet_schema(), p.iter().copied());
                        builder = builder.with_projection(mask);
                    }
                    let reader = builder.build()?;
                    Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(Some(results.into_iter().flatten().collect()))
    }

    fn progress(&self) -> (u64, u64) {
        (self.done_bytes, self.total_bytes)
    }

    fn rows_hint(&self) -> Option<u64> {
        Some(self.num_rows())
    }
}

pub struct IpcSource {
    reader: arrow::ipc::reader::FileReader<BufReader<File>>,
    wave: usize,
    total_bytes: u64,
    batches_done: usize,
    batches_total: usize,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
}

impl IpcSource {
    pub fn open(path: &Path, wave: usize, columns: Option<&[String]>) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::io(e, path))?;
        let total_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let reader = arrow::ipc::reader::FileReader::try_new(BufReader::new(file), None)?;
        let full = reader.schema();
        let projection = match columns {
            None => None,
            Some(cols) => Some(
                cols.iter()
                    .map(|c| full.index_of(c).map_err(|_| Error::Schema(format!("no column named '{c}'"))))
                    .collect::<Result<Vec<_>>>()?,
            ),
        };
        let schema = match &projection {
            None => full,
            Some(p) => Arc::new(full.project(p)?),
        };
        let batches_total = reader.num_batches();
        Ok(IpcSource { reader, wave: wave.max(1), total_bytes, batches_done: 0, batches_total, projection, schema })
    }
}

impl Source for IpcSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_wave(&mut self) -> Result<Option<Vec<RecordBatch>>> {
        let mut out = Vec::new();
        while out.len() < self.wave {
            match self.reader.next() {
                Some(b) => {
                    let b = b?;
                    self.batches_done += 1;
                    out.push(match &self.projection {
                        Some(p) => b.project(p)?,
                        None => b,
                    });
                }
                None => break,
            }
        }
        Ok(if out.is_empty() { None } else { Some(out) })
    }

    fn progress(&self) -> (u64, u64) {
        let frac = if self.batches_total == 0 { 1.0 } else { self.batches_done as f64 / self.batches_total as f64 };
        ((self.total_bytes as f64 * frac) as u64, self.total_bytes)
    }
}

/// JSON Lines: split on newlines (raw newlines cannot occur inside NDJSON values) and
/// decode chunks in parallel against one inferred schema.
pub struct JsonlSource {
    chunker: Chunker<Box<dyn std::io::Read + Send>>,
    schema: SchemaRef,
    wave: usize,
    consumed: Arc<std::sync::atomic::AtomicU64>,
    total: u64,
    pool: Arc<rayon::ThreadPool>,
    done: bool,
}

impl JsonlSource {
    pub fn open(path: &Path, chunk_size: usize, wave: usize, pool: Arc<rayon::ThreadPool>, full_scan: bool) -> Result<Self> {
        let schema = Arc::new(infer_jsonl_schema(path, if full_scan { None } else { Some(64 << 20) })?);
        let input = crate::io::open_input(path)?;
        let chunker = Chunker::new(input.reader, chunk_size, None, b'\n');
        Ok(JsonlSource {
            chunker,
            schema,
            wave: wave.max(1),
            consumed: input.consumed,
            total: input.file_size,
            pool,
            done: false,
        })
    }
}

pub fn infer_jsonl_schema(path: &Path, limit_bytes: Option<u64>) -> Result<Schema> {
    let input = crate::io::open_input(path)?;
    let reader: Box<dyn std::io::Read> = match limit_bytes {
        Some(n) => Box::new(std::io::Read::take(input.reader, n)),
        None => input.reader,
    };
    let mut buf = std::io::BufReader::with_capacity(1 << 20, reader);
    let (schema, _) = arrow::json::reader::infer_json_schema(&mut buf, None)?;
    Ok(schema)
}

impl Source for JsonlSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_wave(&mut self) -> Result<Option<Vec<RecordBatch>>> {
        if self.done {
            return Ok(None);
        }
        let mut chunks = Vec::new();
        while chunks.len() < self.wave {
            match self.chunker.next_chunk()? {
                Some(c) => chunks.push(c),
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        if chunks.is_empty() {
            return Ok(None);
        }
        let schema = self.schema.clone();
        let parts: Vec<Vec<RecordBatch>> = self.pool.install(|| {
            chunks
                .par_iter()
                .map(|c| -> Result<Vec<RecordBatch>> {
                    let reader = arrow::json::ReaderBuilder::new(schema.clone())
                        .with_batch_size(65_536)
                        .build(Cursor::new(&c.data[..]))?;
                    Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(Some(parts.into_iter().flatten().collect()))
    }

    fn progress(&self) -> (u64, u64) {
        (self.consumed.load(std::sync::atomic::Ordering::Relaxed), self.total)
    }
}
