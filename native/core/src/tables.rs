//! Columnar sources and sinks: Parquet, Arrow IPC (Feather v2), JSON Lines and CSV.
//!
//! Sinks receive record batches in order.  The Parquet sink encodes the columns of a
//! row group in parallel (most writers do this on one thread), and can tune each
//! column's encoding by trial for the smallest output.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Date64Type, Field, Schema, SchemaRef, TimeUnit};
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
    .map_err(|e| std::io::Error::other(e.to_string())));

impl FinishWrite for brotli::CompressorWriter<BufWriter<File>> {
    fn finish_box(self: Box<Self>) -> std::io::Result<()> {
        let mut inner = self.into_inner();
        inner.flush()
    }
}

/// Wrap `w` in a compressor chosen by the output path's extension.
pub fn compressed_writer(
    w: BufWriter<File>,
    compression: Compression,
    level: Option<i32>,
    threads: usize,
) -> Result<Box<dyn FinishWrite>> {
    Ok(match compression {
        Compression::None => Box::new(Plain(w)),
        Compression::Gzip => {
            Box::new(flate2::write::GzEncoder::new(w, flate2::Compression::new(level.unwrap_or(6).clamp(0, 9) as u32)))
        }
        Compression::Zstd => {
            let mut enc = zstd::stream::write::Encoder::new(w, level.unwrap_or(3))?;
            if threads > 1 {
                enc.multithread(threads.min(64) as u32)?;
            }
            Box::new(enc)
        }
        Compression::Xz => {
            // Multi-threaded xz: the input is cut into independent blocks.
            let stream = liblzma::stream::MtStreamBuilder::new()
                .threads(threads.clamp(1, 64) as u32)
                .block_size(8 << 20)
                .preset(level.unwrap_or(6).clamp(0, 9) as u32)
                .check(liblzma::stream::Check::Crc64)
                .encoder()
                .map_err(|e| Error::Invalid(format!("xz: {e}")))?;
            Box::new(liblzma::write::XzEncoder::new_stream(w, stream))
        }
        Compression::Lz4 => Box::new(lz4_flex::frame::FrameEncoder::new(w)),
        Compression::Brotli => {
            Box::new(brotli::CompressorWriter::new(w, 1 << 16, level.unwrap_or(6).clamp(0, 11) as u32, 22))
        }
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
    /// Columns written without dictionary encoding (high cardinality); filled in
    /// automatically from the first batch.
    #[serde(skip)]
    pub plain_columns: Vec<String>,
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
            // standard list and map field names, which every reader understands
            .set_coerce_types(true)
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
        for name in &self.plain_columns {
            if schema.field_with_name(name).is_ok() && !self.encodings.contains_key(name) {
                b = b.set_column_dictionary_enabled(ColumnPath::new(vec![name.clone()]), false);
            }
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
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_) => &["DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT", "PLAIN"],
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            &["DICTIONARY", "BYTE_STREAM_SPLIT", "PLAIN"]
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            &["DICTIONARY", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN"]
        }
        _ => &[],
    }
}

/// Columns whose sample is at least 90% distinct values: dictionary encoding would be
/// built and then abandoned, so it is skipped for them.
pub fn high_cardinality_columns(sample: &RecordBatch) -> Vec<String> {
    use arrow::array::{Array, AsArray};
    use std::hash::{BuildHasher, Hash, Hasher};
    let n = sample.num_rows().min(32_768);
    if n < 1024 {
        return Vec::new();
    }
    let state = ahash::RandomState::with_seeds(1, 2, 3, 4);
    let schema = sample.schema();
    let mut out = Vec::new();
    for (field, col) in schema.fields().iter().zip(sample.columns()) {
        let mut seen = ahash::AHashSet::with_capacity(n);
        let mut count = |h: u64| {
            seen.insert(h);
        };
        let hash = |f: &dyn Fn(&mut ahash::AHasher)| {
            let mut h = state.build_hasher();
            f(&mut h);
            h.finish()
        };
        match field.data_type() {
            DataType::Utf8 => {
                let a = col.as_string::<i32>();
                for i in (0..n).filter(|&i| !a.is_null(i)) {
                    count(hash(&|h| a.value(i).hash(h)));
                }
            }
            DataType::Float64 => {
                let a = col.as_primitive::<arrow::datatypes::Float64Type>();
                for i in (0..n).filter(|&i| !a.is_null(i)) {
                    count(a.value(i).to_bits());
                }
            }
            DataType::Int64 => {
                let a = col.as_primitive::<arrow::datatypes::Int64Type>();
                for i in (0..n).filter(|&i| !a.is_null(i)) {
                    count(a.value(i) as u64);
                }
            }
            _ => continue,
        }
        if seen.len() * 10 > n * 9 {
            out.push(field.name().clone());
        }
    }
    out
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
                    if best.is_none_or(|(b, _)| size < b) {
                        best = Some((size, cand));
                    }
                }
            }
            best.map(|(_, enc)| (field.name().clone(), enc.to_string()))
        })
        .collect()
}

type EncodedGroup = std::result::Result<Vec<ArrowColumnChunk>, parquet::errors::ParquetError>;

/// Parquet output whose row groups are encoded on worker threads (every column of a
/// row group in parallel, and up to `max_in_flight` row groups at once) while the
/// finished ones are appended to the file in order.
/// The type a column is stored as in Parquet, when its own has no Parquet form that
/// readers recognise: there is no unit for seconds (such timestamps and times would
/// read back as bare integers) and no 64-bit date.  None when the type is fine.
fn parquet_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::Timestamp(TimeUnit::Second, tz) => Some(DataType::Timestamp(TimeUnit::Millisecond, tz.clone())),
        DataType::Time32(TimeUnit::Second) => Some(DataType::Time32(TimeUnit::Millisecond)),
        DataType::Date64 => Some(DataType::Date32),
        DataType::Struct(fields) => {
            let mapped: Vec<Option<DataType>> = fields.iter().map(|f| parquet_type(f.data_type())).collect();
            mapped.iter().any(Option::is_some).then(|| {
                DataType::Struct(
                    fields
                        .iter()
                        .zip(mapped)
                        .map(|(f, t)| match t {
                            Some(t) => Arc::new(f.as_ref().clone().with_data_type(t)),
                            None => f.clone(),
                        })
                        .collect(),
                )
            })
        }
        DataType::List(f) => {
            parquet_type(f.data_type()).map(|t| DataType::List(Arc::new(f.as_ref().clone().with_data_type(t))))
        }
        DataType::LargeList(f) => {
            parquet_type(f.data_type()).map(|t| DataType::LargeList(Arc::new(f.as_ref().clone().with_data_type(t))))
        }
        _ => None,
    }
}

/// A date64 value with a time of day would lose it as a Parquet date.
fn check_date64(name: &str, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::Date64 => {
            let days = array.as_primitive::<Date64Type>();
            if let Some(v) = days.iter().flatten().find(|v| v.rem_euclid(86_400_000) != 0) {
                return Err(Error::Schema(format!(
                    "column '{name}': date64 value {v} has a time of day, which a Parquet date cannot keep; \
                     cast the column to a timestamp first"
                )));
            }
        }
        DataType::Struct(_) => {
            for c in array.as_struct().columns() {
                check_date64(name, c.as_ref())?;
            }
        }
        DataType::List(_) => check_date64(name, array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => check_date64(name, array.as_list::<i64>().values().as_ref())?,
        _ => {}
    }
    Ok(())
}

pub struct ParquetSink {
    writer: Option<SerializedFileWriter<BufWriter<File>>>,
    factory: Arc<ArrowRowGroupWriterFactory>,
    schema: SchemaRef,
    /// Per column, the type it is stored as when that differs (see `parquet_type`).
    casts: Vec<Option<DataType>>,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    row_group_rows: usize,
    next_group: usize,
    appended: usize,
    in_flight: usize,
    max_in_flight: usize,
    done_tx: std::sync::mpsc::Sender<(usize, EncodedGroup)>,
    done_rx: std::sync::mpsc::Receiver<(usize, EncodedGroup)>,
    finished: std::collections::BTreeMap<usize, EncodedGroup>,
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
        let casts: Vec<Option<DataType>> = schema.fields().iter().map(|f| parquet_type(f.data_type())).collect();
        let schema = if casts.iter().any(Option::is_some) {
            let fields: Vec<Field> = schema
                .fields()
                .iter()
                .zip(&casts)
                .map(|(f, t)| match t {
                    Some(t) => f.as_ref().clone().with_data_type(t.clone()),
                    None => f.as_ref().clone(),
                })
                .collect();
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        } else {
            schema
        };
        let options = ArrowWriterOptions::new().with_properties(props);
        let writer = ArrowWriter::try_new_with_options(file, schema.clone(), options)?;
        let (writer, factory) = writer.into_serialized_writer()?;
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        Ok(ParquetSink {
            writer: Some(writer),
            factory: Arc::new(factory),
            schema,
            casts,
            pending: Vec::new(),
            pending_rows: 0,
            row_group_rows: row_group_rows.max(1),
            next_group: 0,
            appended: 0,
            in_flight: 0,
            max_in_flight: 2,
            done_tx,
            done_rx,
            finished: std::collections::BTreeMap::new(),
            pool,
        })
    }

    /// Hand the pending batches to the workers as the next row group.
    fn dispatch(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        while self.in_flight >= self.max_in_flight {
            self.wait_one()?;
        }
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let index = self.next_group;
        self.next_group += 1;
        self.in_flight += 1;
        let (factory, schema, pool, tx) =
            (self.factory.clone(), self.schema.clone(), self.pool.clone(), self.done_tx.clone());
        self.pool.spawn(move || {
            let encode = || -> EncodedGroup {
                let writers = factory.create_column_writers(index)?;
                let mut jobs: Vec<(_, Vec<_>)> = writers.into_iter().map(|w| (w, Vec::new())).collect();
                for batch in &batches {
                    let mut leaf = 0;
                    for (field, column) in schema.fields().iter().zip(batch.columns()) {
                        for l in compute_leaves(field, column)? {
                            jobs[leaf].1.push(l);
                            leaf += 1;
                        }
                    }
                }
                drop(batches);
                pool.install(|| {
                    jobs.into_par_iter()
                        .map(|(mut w, leaves)| {
                            for l in &leaves {
                                w.write(l)?;
                            }
                            w.close()
                        })
                        .collect()
                })
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(encode)).unwrap_or_else(|_| {
                Err(parquet::errors::ParquetError::General("internal error while encoding a row group".into()))
            });
            let _ = tx.send((index, result));
        });
        self.append_ready()
    }

    fn wait_one(&mut self) -> Result<()> {
        let (index, result) =
            self.done_rx.recv().map_err(|_| Error::Data("a Parquet encoding worker stopped".into()))?;
        self.in_flight -= 1;
        self.finished.insert(index, result);
        self.append_ready()
    }

    /// Append every finished row group that is next in file order.
    fn append_ready(&mut self) -> Result<()> {
        while let Ok((index, result)) = self.done_rx.try_recv() {
            self.in_flight -= 1;
            self.finished.insert(index, result);
        }
        while let Some(result) = self.finished.remove(&self.appended) {
            let chunks = result?;
            let writer = self.writer.as_mut().expect("writer open");
            let mut rg = writer.next_row_group()?;
            for chunk in chunks {
                chunk.append_to_row_group(&mut rg)?;
            }
            rg.close()?;
            self.appended += 1;
        }
        Ok(())
    }
}

impl Sink for ParquetSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let converted;
        let batch = if self.casts.iter().any(Option::is_some) {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for ((column, cast), field) in batch.columns().iter().zip(&self.casts).zip(batch.schema().fields()) {
                columns.push(match cast {
                    Some(t) => {
                        check_date64(field.name(), column.as_ref())?;
                        arrow::compute::cast(column, t)?
                    }
                    None => column.clone(),
                });
            }
            converted = RecordBatch::try_new(self.schema.clone(), columns)?;
            &converted
        } else {
            batch
        };
        let mut offset = 0;
        let rows = batch.num_rows();
        while offset < rows {
            let room = self.row_group_rows - self.pending_rows;
            let take = room.min(rows - offset);
            self.pending.push(batch.slice(offset, take));
            self.pending_rows += take;
            offset += take;
            if self.pending_rows >= self.row_group_rows {
                self.dispatch()?;
            }
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.dispatch()?;
        while self.in_flight > 0 {
            self.wait_one()?;
        }
        self.append_ready()?;
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
            .with_encoder_factory(Arc::new(crate::jsonl::FloatText))
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
    /// Notes about the input's types (e.g. JSON fields that mix value types).
    fn warnings(&self) -> Vec<String> {
        Vec::new()
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
    pub fn open(
        path: &Path,
        wave_bytes: u64,
        max_parallel: usize,
        pool: Arc<rayon::ThreadPool>,
        columns: Option<&[String]>,
    ) -> Result<Self> {
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

enum IpcLayout {
    File(arrow::ipc::reader::FileReader<BufReader<File>>),
    Stream(arrow::ipc::reader::StreamReader<BufReader<File>>),
}

/// The record batches of an Arrow IPC file in either layout: the file format (which
/// starts with `ARROW1` and has an index) or the stream format.
pub struct IpcBatches {
    layout: IpcLayout,
    pub schema: SchemaRef,
    /// Batches in the file; the stream format has no index.
    pub batches: Option<usize>,
}

impl IpcBatches {
    pub fn open(path: &Path) -> Result<Self> {
        let head = crate::io::read_head(path, 6)?;
        let file = BufReader::new(File::open(path).map_err(|e| Error::io(e, path))?);
        if head == b"ARROW1" {
            let r = arrow::ipc::reader::FileReader::try_new(file, None)?;
            let (schema, batches) = (r.schema(), Some(r.num_batches()));
            Ok(IpcBatches { layout: IpcLayout::File(r), schema, batches })
        } else {
            let r = arrow::ipc::reader::StreamReader::try_new(file, None)?;
            Ok(IpcBatches { schema: r.schema(), layout: IpcLayout::Stream(r), batches: None })
        }
    }
}

impl Iterator for IpcBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        let next = match &mut self.layout {
            IpcLayout::File(r) => r.next(),
            IpcLayout::Stream(r) => r.next(),
        };
        next.map(|b| b.map_err(Error::from))
    }
}

pub struct IpcSource {
    reader: IpcBatches,
    wave: usize,
    total_bytes: u64,
    batches_done: usize,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    done: bool,
}

impl IpcSource {
    pub fn open(path: &Path, wave: usize, columns: Option<&[String]>) -> Result<Self> {
        let total_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let reader = IpcBatches::open(path)?;
        let full = reader.schema.clone();
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
        Ok(IpcSource { reader, wave: wave.max(1), total_bytes, batches_done: 0, projection, schema, done: false })
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
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        Ok(if out.is_empty() { None } else { Some(out) })
    }

    fn progress(&self) -> (u64, u64) {
        let frac = match self.reader.batches {
            _ if self.done => 1.0,
            Some(n) if n > 0 => self.batches_done as f64 / n as f64,
            _ => 0.0,
        };
        ((self.total_bytes as f64 * frac) as u64, self.total_bytes)
    }
}

/// JSON Lines: split on newlines (raw newlines cannot occur inside NDJSON values) and
/// decode chunks in parallel against one inferred layout (see [`crate::jsonl`]).
pub struct JsonlSource {
    chunker: Chunker<Box<dyn std::io::Read + Send>>,
    layout: Arc<crate::jsonl::Layout>,
    schema: SchemaRef,
    warnings: Vec<String>,
    wave: usize,
    consumed: Arc<std::sync::atomic::AtomicU64>,
    total: u64,
    pool: Arc<rayon::ThreadPool>,
    done: bool,
}

impl JsonlSource {
    /// Infer types from the first `sample` bytes, or from the whole file with None.
    pub fn open(
        path: &Path,
        chunk_size: usize,
        wave: usize,
        pool: Arc<rayon::ThreadPool>,
        sample: Option<u64>,
    ) -> Result<Self> {
        let head = crate::io::open_input(path)?;
        let (layout, warnings) = pool.install(|| crate::jsonl::infer(head.reader, sample))?;
        let schema = layout.schema();
        let input = crate::io::open_input(path)?;
        let reader: Box<dyn std::io::Read + Send> = Box::new(crate::io::SkipBom::new(input.reader));
        let chunker = Chunker::new(reader, chunk_size, None, b'\n');
        Ok(JsonlSource {
            chunker,
            layout: Arc::new(layout),
            schema,
            warnings,
            wave: wave.max(1),
            consumed: input.consumed,
            total: input.file_size,
            pool,
            done: false,
        })
    }
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
        let (layout, schema) = (self.layout.clone(), self.schema.clone());
        let parts: Vec<Option<RecordBatch>> = self.pool.install(|| {
            chunks.par_iter().map(|c| crate::jsonl::decode(&layout, &schema, &c.data, c.offset)).collect::<Result<_>>()
        })?;
        Ok(Some(parts.into_iter().flatten().collect()))
    }

    fn warnings(&self) -> Vec<String> {
        self.warnings.clone()
    }

    fn progress(&self) -> (u64, u64) {
        (self.consumed.load(std::sync::atomic::Ordering::Relaxed), self.total)
    }
}
