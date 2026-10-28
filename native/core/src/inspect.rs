//! File inspection: `info`, `head`, `count` and `schema` without loading whole files.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use arrow::array::{Array, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use serde_json::{json, Value};

use crate::chunk::{count_records, Chunker};
use crate::convert::{self, detect_format, ConvertOptions, Format};
use crate::error::{Error, Result};
use crate::io;
use crate::types::{ColumnStats, TypesMode};

#[derive(Serialize)]
pub struct Preview {
    pub columns: Vec<String>,
    pub types: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

fn preview_from_batches(batches: &[RecordBatch], n: usize) -> Result<Preview> {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => return Ok(Preview { columns: vec![], types: vec![], rows: vec![] }),
    };
    let fmt = FormatOptions::default().with_null("");
    let mut rows = Vec::new();
    'outer: for b in batches {
        let formatters: Vec<ArrayFormatter> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &fmt))
            .collect::<std::result::Result<_, _>>()?;
        for r in 0..b.num_rows() {
            if rows.len() >= n {
                break 'outer;
            }
            rows.push(
                b.columns()
                    .iter()
                    .zip(&formatters)
                    .map(|(c, f)| if c.is_null(r) { None } else { Some(f.value(r).to_string()) })
                    .collect(),
            );
        }
    }
    Ok(Preview {
        columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
        types: schema.fields().iter().map(|f| f.data_type().to_string()).collect(),
        rows,
    })
}

/// First `n` rows of any supported file, formatted as text.
pub fn head(path: &Path, n: usize, opts: &ConvertOptions) -> Result<Preview> {
    let (fmt, _) = detect_format(path, opts.from.as_deref())?;
    match fmt {
        Format::Parquet => {
            let file = File::open(path).map_err(|e| Error::io(e, path))?;
            let meta = crate::tables::parquet_metadata(&file)?;
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta)
                .with_batch_size(n.max(1))
                .with_limit(n)
                .build()?;
            let batches: Vec<RecordBatch> = reader.take(1).collect::<std::result::Result<_, _>>()?;
            preview_from_batches(&batches, n)
        }
        Format::Ipc => {
            let reader = crate::tables::IpcBatches::open(path)?;
            let mut batches = Vec::new();
            let mut rows = 0;
            for b in reader {
                let b = b?;
                rows += b.num_rows();
                batches.push(b);
                if rows >= n {
                    break;
                }
            }
            preview_from_batches(&batches, n)
        }
        Format::Jsonl => {
            let input = io::open_input(path)?;
            let mut reader = input.reader;
            let head = io::read_up_to(&mut reader, 8 << 20)?;
            let lines: Vec<&[u8]> =
                head.split(|&b| b == b'\n').filter(|l| l.iter().any(|b| !b.is_ascii_whitespace())).take(n).collect();
            let text = lines.join(&b'\n');
            let batches: Vec<RecordBatch> = crate::jsonl::read_text(&text)?.into_iter().collect();
            preview_from_batches(&batches, n)
        }
        Format::Csv | Format::Dcz => {
            // Convert the first rows to Arrow IPC in memory, through the regular
            // pipeline so the preview shows exactly the types a conversion would use.
            let dir = std::env::temp_dir();
            let tmp = io::temp_sibling(&dir.join("dataconv-head.arrow"), "head");
            let mut o = opts.clone();
            o.limit = Some(n as u64);
            o.to = Some("ipc".into());
            o.compression = Some("none".into());
            o.overwrite = true;
            o.bad_lines = convert::BadLines::Skip;
            let result = convert::convert(path, &tmp, &o, &convert::NoHooks).and_then(|_| {
                let file = File::open(&tmp).map_err(|e| Error::io(e, &tmp))?;
                let reader = arrow::ipc::reader::FileReader::try_new(BufReader::new(file), None)?;
                let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
                preview_from_batches(&batches, n)
            });
            let _ = std::fs::remove_file(&tmp);
            result
        }
    }
}

/// Exact number of data rows.
pub fn count(path: &Path, opts: &ConvertOptions) -> Result<u64> {
    let (fmt, _) = detect_format(path, opts.from.as_deref())?;
    match fmt {
        Format::Parquet => {
            let reader = SerializedFileReader::new(File::open(path).map_err(|e| Error::io(e, path))?)?;
            Ok(reader.metadata().file_metadata().num_rows().max(0) as u64)
        }
        Format::Ipc => {
            let mut rows = 0u64;
            for b in crate::tables::IpcBatches::open(path)? {
                rows += b?.num_rows() as u64;
            }
            Ok(rows)
        }
        Format::Jsonl => {
            let input = io::open_input(path)?;
            let mut chunker = Chunker::new(input.reader, 8 << 20, None, b'\n');
            let mut rows = 0u64;
            while let Some(c) = chunker.next_chunk()? {
                rows +=
                    c.data.split(|&b| b == b'\n').filter(|l| l.iter().any(|b| !b.is_ascii_whitespace())).count() as u64;
            }
            Ok(rows)
        }
        Format::Csv | Format::Dcz => {
            let dialect = convert::sniff_file(path, &opts.sniff)?;
            let td = dialect.tokenizer_dialect();
            let (reader, _, _) = convert::data_stream(path, &dialect)?;
            // blank lines are skipped like the converters do, except in a single-column
            // file, where they are rows with a missing value
            count_records(reader, td, dialect.split_byte(), dialect.columns.len() == 1)
        }
    }
}

fn parquet_info(path: &Path) -> Result<Value> {
    let reader = SerializedFileReader::new(File::open(path).map_err(|e| Error::io(e, path))?)?;
    let md = reader.metadata();
    let fm = md.file_metadata();
    let schema = fm.schema_descr();
    let mut columns = Vec::new();
    for (i, col) in schema.columns().iter().enumerate() {
        let mut compressed = 0i64;
        let mut uncompressed = 0i64;
        let mut nulls: Option<u64> = Some(0);
        let mut codecs = std::collections::BTreeSet::new();
        let mut encodings = std::collections::BTreeSet::new();
        let mut min: Option<String> = None;
        let mut max: Option<String> = None;
        for rg in md.row_groups() {
            let c = rg.column(i);
            compressed += c.compressed_size();
            uncompressed += c.uncompressed_size();
            codecs.insert(crate::tables::describe_codec(&c.compression()));
            for e in c.encodings() {
                encodings.insert(format!("{e:?}"));
            }
            match c.statistics() {
                Some(s) => {
                    nulls = match (nulls, s.null_count_opt()) {
                        (Some(a), Some(b)) => Some(a + b),
                        _ => None,
                    };
                    if rg.ordinal() == Some(0) || min.is_none() {
                        min = stat_text(s, true);
                        max = stat_text(s, false);
                    }
                }
                None => nulls = None,
            }
        }
        columns.push(json!({
            "name": col.path().string(),
            "physical_type": format!("{}", col.physical_type()),
            "logical_type": col.logical_type_ref().map(|t| format!("{t:?}")),
            "compression": codecs.into_iter().collect::<Vec<_>>(),
            "encodings": encodings.into_iter().collect::<Vec<_>>(),
            "compressed_bytes": compressed,
            "uncompressed_bytes": uncompressed,
            "null_count": nulls,
            "min_first_row_group": min,
            "max_first_row_group": max,
        }));
    }
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let uncompressed: i64 = md.row_groups().iter().map(|rg| rg.total_byte_size()).sum();
    Ok(json!({
        "format": "parquet",
        "path": path.display().to_string(),
        "file_size": size,
        "rows": fm.num_rows(),
        "row_groups": md.num_row_groups(),
        "columns": columns,
        "created_by": fm.created_by(),
        "format_version": fm.version(),
        "uncompressed_bytes": uncompressed,
        "metadata_keys": fm.key_value_metadata().map(|kv| kv.iter().map(|k| k.key.clone()).collect::<Vec<_>>()),
    }))
}

fn stat_text(s: &parquet::file::statistics::Statistics, min: bool) -> Option<String> {
    use parquet::file::statistics::Statistics as S;
    let text = match s {
        S::Boolean(v) => (if min { v.min_opt() } else { v.max_opt() }).map(|x| x.to_string()),
        S::Int32(v) => (if min { v.min_opt() } else { v.max_opt() }).map(|x| x.to_string()),
        S::Int64(v) => (if min { v.min_opt() } else { v.max_opt() }).map(|x| x.to_string()),
        S::Float(v) => (if min { v.min_opt() } else { v.max_opt() }).map(|x| x.to_string()),
        S::Double(v) => (if min { v.min_opt() } else { v.max_opt() }).map(|x| x.to_string()),
        S::ByteArray(v) => {
            (if min { v.min_opt() } else { v.max_opt() }).map(|x| String::from_utf8_lossy(x.data()).into_owned())
        }
        _ => None,
    };
    text.map(|t| {
        if t.len() > 64 {
            format!("{}…", &t[..t.char_indices().nth(60).map_or(t.len(), |(i, _)| i)])
        } else {
            t
        }
    })
}

/// Describe a file: format, size, rows, schema and format-specific details.
pub fn info(path: &Path, exact: bool, opts: &ConvertOptions) -> Result<Value> {
    let (fmt, comp) = detect_format(path, opts.from.as_deref())?;
    let size = std::fs::metadata(path).map_err(|e| Error::io(e, path))?.len();
    match fmt {
        Format::Parquet => parquet_info(path),
        Format::Ipc => {
            let reader = crate::tables::IpcBatches::open(path)?;
            let schema = reader.schema.clone();
            let layout = if reader.batches.is_some() { "file" } else { "stream" };
            let batches = match reader.batches {
                Some(n) => n,
                None => reader.count(),
            };
            let rows = if exact { Some(count(path, opts)?) } else { None };
            Ok(json!({
                "format": "ipc", "layout": layout, "path": path.display().to_string(), "file_size": size,
                "rows": rows, "record_batches": batches,
                "columns": schema.fields().iter().map(|f| json!({"name": f.name(), "type": f.data_type().to_string(), "nullable": f.is_nullable()})).collect::<Vec<_>>(),
            }))
        }
        Format::Dcz => {
            let footer = crate::dcz::read_footer(path)?;
            let mut v = serde_json::to_value(&footer)?;
            let blocks = footer.blocks.len();
            let columnar = footer.blocks.iter().filter(|b| b.kind == crate::dcz::KIND_CSV).count();
            if let Value::Object(map) = &mut v {
                map.remove("blocks");
                map.insert("path".into(), json!(path.display().to_string()));
                map.insert("file_size".into(), json!(size));
                map.insert("block_count".into(), json!(blocks));
                map.insert("columnar_blocks".into(), json!(columnar));
                map.insert("ratio".into(), json!(footer.original_size as f64 / size.max(1) as f64));
            }
            Ok(v)
        }
        Format::Csv | Format::Jsonl => {
            let mut o = opts.clone();
            o.limit = Some(1000);
            let preview = head(path, 1000, &o)?;
            let rows = if exact { Some(count(path, opts)?) } else { None };
            let dialect = if fmt == Format::Csv { Some(convert::sniff_file(path, &opts.sniff)?) } else { None };
            Ok(json!({
                "format": fmt.name(),
                "path": path.display().to_string(),
                "file_size": size,
                "compression": comp.name(),
                "rows": rows,
                "dialect": dialect,
                "columns": preview.columns.iter().zip(&preview.types).map(|(n, t)| json!({"name": n, "type": t})).collect::<Vec<_>>(),
            }))
        }
    }
}

/// Column statistics of a CSV file over its full contents (used by `profile`).
pub fn csv_column_stats(path: &Path, opts: &ConvertOptions) -> Result<Vec<(String, ColumnStats)>> {
    let dialect = convert::sniff_file(path, &opts.sniff)?;
    let td = dialect.tokenizer_dialect();
    let (reader, _, _) = convert::data_stream(path, &dialect)?;
    let mut chunker = Chunker::new(reader, 16 << 20, td.quote, dialect.split_byte()).with_dialect(td);
    let ncols = dialect.columns.len();
    let mut stats = vec![ColumnStats::default(); ncols];
    let mut scratch = Vec::new();
    let mut tok = crate::tokenize::Tokenizer::new(td);
    while let Some(c) = chunker.next_chunk()? {
        tok.for_each(&c.data, |r| {
            if r.fields.len() != ncols || r.suspect() {
                return true;
            }
            for (j, f) in r.fields.iter().enumerate() {
                let v = crate::tokenize::unescape(&r.data[f.start..f.end], f, &td, &mut scratch);
                stats[j].observe(v, TypesMode::Infer, &[]);
            }
            true
        });
    }
    Ok(dialect.columns.into_iter().zip(stats).collect())
}
