use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimeUnit};
use dataconv_core::convert::{convert, BadLines, ConvertOptions, NoHooks};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn dir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("dcv-it-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn read_parquet(path: &PathBuf) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap().build().unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn tricky_csv() -> String {
    let mut s = String::from("id,zip,big,price,precise,when,day,ok,name,note\n");
    for i in 0..5000 {
        let zip = format!("{:05}", (i * 7919) % 100000);
        let big = if i == 17 { "18446744073709551615".to_string() } else { (i as u64 * 1_000_003).to_string() };
        let price = if i % 97 == 0 { "NA".to_string() } else { format!("{}.{:02}", i % 1000, i % 100) };
        let precise = if i == 3 { "1.00000000000000000001".to_string() } else { format!("{}.5", i) };
        let when = format!("2024-01-02T03:{:02}:{:02}.{:03}+05:30", i % 60, (i * 7) % 60, i % 1000);
        let day = format!("2023-{:02}-{:02}", i % 12 + 1, i % 28 + 1);
        let ok = if i % 2 == 0 { "true" } else { "False" };
        let name = match i % 4 {
            0 => "\"Smith, J\"".to_string(),
            1 => "NA".to_string(),
            2 => "\"line\nbreak\"".to_string(),
            _ => format!("user{i}"),
        };
        let note = if i % 3 == 0 { String::new() } else { "\"\"".to_string() };
        s.push_str(&format!("{i},{zip},{big},{price},{precise},{when},{day},{ok},{name},{note}\n"));
        if i == 2500 {
            s.push_str("this,row,is,broken\n\n");
        }
    }
    s
}

#[test]
fn csv_to_parquet_is_lossless() {
    let d = dir();
    let src = d.join("tricky.csv");
    std::fs::write(&src, tricky_csv()).unwrap();
    let dst = d.join("tricky.parquet");
    let opts = ConvertOptions { overwrite: true, chunk_size: Some(64 << 10), ..Default::default() };
    let report = convert(&src, &dst, &opts, &NoHooks).unwrap();
    assert_eq!(report.rows, 5000);
    assert_eq!(report.bad_rows, 1);
    assert_eq!(report.blank_lines, 1);
    let quarantine = report.quarantine.clone().unwrap();
    let rejected = std::fs::read_to_string(&quarantine).unwrap();
    assert!(rejected.contains("this,row,is,broken"));
    let types: Vec<(String, String)> = report.schema.clone();
    let ty = |n: &str| types.iter().find(|(c, _)| c == n).unwrap().1.clone();
    assert_eq!(ty("id"), "int64");
    assert_eq!(ty("zip"), "string", "leading zeros must survive");
    assert_eq!(ty("big"), "decimal(20,0)");
    assert_eq!(ty("price"), "float64");
    assert_eq!(ty("precise"), "decimal(24,20)");
    assert_eq!(ty("when"), "timestamp[ms,+05:30]");
    assert_eq!(ty("day"), "date");
    assert_eq!(ty("ok"), "bool");
    assert_eq!(ty("name"), "string");
    assert!(report.warnings.iter().any(|w| w.contains("zip") && w.contains("leading zeros")), "{:?}", report.warnings);

    let batch = read_parquet(&dst);
    assert_eq!(batch.num_rows(), 5000);
    let zip = batch.column_by_name("zip").unwrap().as_string::<i32>();
    assert_eq!(zip.value(0), "00000");
    assert_eq!(zip.value(1), "07919");
    let price = batch.column_by_name("price").unwrap().as_primitive::<Float64Type>();
    assert!(price.is_null(0), "NA in a numeric column is missing");
    assert_eq!(price.value(5), 5.05);
    let name = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(name.value(1), "NA", "NA in a text column is kept");
    assert_eq!(name.value(2), "line\nbreak");
    let note = batch.column_by_name("note").unwrap().as_string::<i32>();
    assert!(note.is_null(0), "unquoted empty is missing");
    assert_eq!(note.value(1), "", "quoted empty is an empty string");
    let when = batch.column_by_name("when").unwrap();
    assert_eq!(when.data_type(), &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+05:30"))));
    let ids = batch.column_by_name("id").unwrap().as_primitive::<Int64Type>();
    assert_eq!(ids.value(4999), 4999);
}

#[test]
fn every_format_roundtrips_through_csv() {
    let d = dir();
    let src = d.join("fmt.csv");
    let csv = tricky_csv().replace("this,row,is,broken\n\n", "");
    std::fs::write(&src, &csv).unwrap();
    for (ext, extra) in [("parquet", ""), ("arrow", ""), ("jsonl", ""), ("csv.zst", ""), ("dcz", ""), ("csv.gz", "")] {
        let mid = d.join(format!("fmt.{ext}"));
        let back = d.join(format!("fmt-back-{}.csv", ext.replace('.', "-")));
        let opts = ConvertOptions { overwrite: true, ..Default::default() };
        convert(&src, &mid, &opts, &NoHooks).unwrap_or_else(|e| panic!("to {ext}{extra}: {e}"));
        convert(&mid, &back, &opts, &NoHooks).unwrap_or_else(|e| panic!("from {ext}: {e}"));
        let again = d.join(format!("fmt-again-{}.parquet", ext.replace('.', "-")));
        convert(&back, &again, &opts, &NoHooks).unwrap();
        let a = read_parquet(&again);
        assert_eq!(a.num_rows(), 5000, "{ext}");
        let zip = a.column_by_name("zip").unwrap().as_string::<i32>();
        assert_eq!(zip.value(1), "07919", "{ext}");
    }
    // CSV -> DCZ -> CSV is byte-exact
    let dcz = d.join("exact.dcz");
    let back = d.join("exact.csv");
    let opts = ConvertOptions { overwrite: true, ..Default::default() };
    convert(&src, &dcz, &opts, &NoHooks).unwrap();
    convert(&dcz, &back, &opts, &NoHooks).unwrap();
    assert_eq!(std::fs::read(&back).unwrap(), csv.as_bytes());
}

#[test]
fn recovers_from_type_drift_beyond_the_sample() {
    let d = dir();
    let src = d.join("drift.csv");
    let mut s = String::from("a,b\n");
    for i in 0..200_000 {
        s.push_str(&format!("{i},{}\n", i % 10));
    }
    s.push_str("1.5,x\n");
    std::fs::write(&src, &s).unwrap();
    let dst = d.join("drift.parquet");
    let opts = ConvertOptions { overwrite: true, sample_bytes: Some(64 << 10), chunk_size: Some(256 << 10), ..Default::default() };
    let report = convert(&src, &dst, &opts, &NoHooks).unwrap();
    assert_eq!(report.restarts, 1, "{:?}", report.warnings);
    assert_eq!(report.schema, vec![("a".into(), "float64".into()), ("b".into(), "string".into())]);
    let batch = read_parquet(&dst);
    assert_eq!(batch.num_rows(), 200_001);
}

#[test]
fn falls_back_to_windows_1252_late_in_the_file() {
    let d = dir();
    let src = d.join("latin.csv");
    let mut bytes = b"city,n\n".to_vec();
    for i in 0..300_000 {
        bytes.extend_from_slice(format!("Paris,{i}\n").as_bytes());
    }
    bytes.extend_from_slice(b"Z\xfcrich,1\n");
    std::fs::write(&src, &bytes).unwrap();
    let dst = d.join("latin.parquet");
    let report = convert(&src, &dst, &ConvertOptions { overwrite: true, ..Default::default() }, &NoHooks).unwrap();
    assert!(report.restarts >= 1);
    let batch = read_parquet(&dst);
    let city = batch.column_by_name("city").unwrap().as_string::<i32>();
    assert_eq!(city.value(300_000), "Zürich");
}

#[test]
fn bad_line_policies_and_options() {
    let d = dir();
    let src = d.join("bad.csv");
    std::fs::write(&src, "a,b,c\n1,2,3\n4,5\n6,7,8,9\n").unwrap();
    let dst = d.join("bad.parquet");
    let err = convert(&src, &dst, &ConvertOptions { overwrite: true, bad_lines: BadLines::Error, ..Default::default() }, &NoHooks);
    assert!(err.is_err());
    assert!(!dst.exists(), "no partial output on failure");
    let r = convert(&src, &dst, &ConvertOptions { overwrite: true, bad_lines: BadLines::Pad, ..Default::default() }, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (2, 1));
    let r = convert(&src, &dst, &ConvertOptions { overwrite: true, bad_lines: BadLines::Skip, ..Default::default() }, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (1, 2));
    assert!(r.quarantine.is_none());
    // projection, limit, schema override, refusing to overwrite
    let opts = ConvertOptions {
        overwrite: true,
        columns: Some(vec!["c".into(), "a".into()]),
        schema: [("a".to_string(), "string".to_string())].into_iter().collect(),
        bad_lines: BadLines::Skip,
        ..Default::default()
    };
    let r = convert(&src, &dst, &opts, &NoHooks).unwrap();
    assert_eq!(r.schema, vec![("c".into(), "int64".into()), ("a".into(), "string".into())]);
    assert!(convert(&src, &dst, &ConvertOptions::default(), &NoHooks).is_err());
}

#[test]
fn max_profile_tunes_parquet() {
    let d = dir();
    let src = d.join("tune.csv");
    let mut s = String::from("ts,v,cat\n");
    for i in 0..100_000 {
        s.push_str(&format!("{},{}.{},{}\n", 1_700_000_000 + i * 15, i % 50, i % 10, ["a", "b", "c"][i % 3]));
    }
    std::fs::write(&src, &s).unwrap();
    let fast = d.join("fast.parquet");
    let max = d.join("max.parquet");
    let rf = convert(&src, &fast, &ConvertOptions { overwrite: true, profile: Some("fast".into()), ..Default::default() }, &NoHooks).unwrap();
    let rm = convert(&src, &max, &ConvertOptions { overwrite: true, profile: Some("max".into()), ..Default::default() }, &NoHooks).unwrap();
    assert!(rm.bytes_out <= rf.bytes_out, "max {} vs fast {}", rm.bytes_out, rf.bytes_out);
    assert_eq!(read_parquet(&max).num_rows(), 100_000);
}

#[test]
fn split_csv_is_byte_exact() {
    let d = dir().join("split-exact");
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let src = d.join("in.csv");
    let mut body = String::new();
    for i in 0..10_000 {
        body.push_str(&format!("{i},\"q, {i}\",\"multi\nline\"\r\n"));
    }
    let header = "a,b,c\r\n";
    std::fs::write(&src, format!("{header}{body}")).unwrap();
    let out = d.join("parts");
    let opts = ConvertOptions { split_rows: Some(3000), split_pattern: Some("chunk_{n:04}".into()), ..Default::default() };
    let r = convert(&src, &out, &opts, &NoHooks).unwrap();
    assert_eq!(r.parts.len(), 4);
    assert_eq!(r.rows, 10_000);
    let mut joined = String::new();
    for (i, p) in r.parts.iter().enumerate() {
        assert!(p.path.ends_with(&format!("chunk_{i:04}.csv")), "{}", p.path);
        let text = std::fs::read_to_string(&p.path).unwrap();
        assert!(text.starts_with(header));
        joined.push_str(&text[header.len()..]);
    }
    assert_eq!(joined, body);
}

#[test]
fn split_by_size_lands_near_target() {
    let d = dir().join("split-size");
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let src = d.join("in.csv");
    let mut s = String::from("id,v,name\n");
    for i in 0..400_000 {
        s.push_str(&format!("{i},{}.{},name{}\n", i % 1000, i % 7, i % 5000));
    }
    std::fs::write(&src, &s).unwrap();
    let out = d.join("pq");
    let target = 256 * 1024u64;
    let opts = ConvertOptions { split_bytes: Some(target), to: Some("parquet".into()), ..Default::default() };
    let r = convert(&src, &out, &opts, &NoHooks).unwrap();
    assert_eq!(r.rows, 400_000);
    assert!(r.parts.len() >= 3, "{} parts", r.parts.len());
    // all but the last part should be within 40% of the target
    for p in &r.parts[..r.parts.len() - 1] {
        let ratio = p.bytes as f64 / target as f64;
        assert!((0.6..1.4).contains(&ratio), "part {} is {} bytes ({ratio:.2}x target)", p.path, p.bytes);
    }
    let total: i64 = r.parts.iter().map(|p| read_parquet(&PathBuf::from(&p.path)).num_rows() as i64).sum();
    assert_eq!(total, 400_000);
}
