//! Edge cases that once lost or corrupted data: escape-aware chunking, headers longer
//! than a chunk, single-column nulls, split atomicity, dialect options, and corrupt
//! DCZ input.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use dataconv_core::codec::Profile;
use dataconv_core::convert::{convert, ConvertOptions, NoHooks};
use dataconv_core::dcz;
use dataconv_core::sniff::SniffOptions;
use dataconv_core::tokenize::Dialect;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("dcv-rb-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn read_parquet(path: &Path) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap().build().unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn opts() -> ConvertOptions {
    ConvertOptions { overwrite: true, ..Default::default() }
}

fn count(path: &Path, o: &ConvertOptions) -> u64 {
    dataconv_core::inspect::count(path, o).unwrap()
}

#[test]
fn escaped_quotes_never_split_a_record() {
    // Every record holds `\"` before an embedded line break: counting quotes alone
    // would end the record at that line break.
    let d = dir("esc");
    let src = d.join("esc.csv");
    let mut s = String::from("id,text,n\n");
    for i in 0..40_000 {
        s.push_str(&format!("{i},\"a \\\" b\nc{i}\",{}\n", i % 7));
    }
    std::fs::write(&src, &s).unwrap();
    let dst = d.join("esc.parquet");
    let o = ConvertOptions { chunk_size: Some(64 << 10), sample_bytes: Some(64 << 10), ..opts() };
    let r = convert(&src, &dst, &o, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (40_000, 0), "{:?}", r.warnings);
    assert_eq!(r.schema[0], ("id".to_string(), "int64".to_string()));
    let b = read_parquet(&dst);
    let text = b.column_by_name("text").unwrap().as_string::<i32>();
    assert!(text.value(39_999).ends_with("\nc39999"), "{:?}", text.value(39_999));
    assert_eq!(count(&src, &o), 40_000);
}

#[test]
fn a_header_longer_than_a_chunk_is_skipped() {
    let d = dir("wide");
    let src = d.join("wide.csv");
    let ncols = 8000;
    let mut s = String::from("exported by some tool\n");
    s.push_str(&(0..ncols).map(|i| format!("a_rather_long_column_name_{i:06}")).collect::<Vec<_>>().join(","));
    s.push('\n');
    for r in 0..3 {
        s.push_str(&(0..ncols).map(|i| (i * 3 + r).to_string()).collect::<Vec<_>>().join(","));
        s.push('\n');
    }
    std::fs::write(&src, &s).unwrap();
    let o = ConvertOptions {
        chunk_size: Some(64 << 10),
        sniff: SniffOptions { skip_rows: Some(1), ..Default::default() },
        ..opts()
    };
    let r = convert(&src, &d.join("wide.parquet"), &o, &NoHooks).unwrap();
    assert_eq!(r.rows, 3);
    assert_eq!(r.schema[7], ("a_rather_long_column_name_000007".to_string(), "int64".to_string()));
    assert_eq!(count(&src, &o), 3);
}

#[test]
fn single_column_nulls_survive_csv() {
    let d = dir("one");
    let src = d.join("one.csv");
    std::fs::write(&src, "v\n1\n\n3\n").unwrap();
    let pq = d.join("one.parquet");
    let r = convert(&src, &pq, &opts(), &NoHooks).unwrap();
    assert_eq!(r.rows, 3);
    let v = read_parquet(&pq);
    let v = v.column(0).as_primitive::<Int64Type>();
    assert!(v.is_null(1));
    assert_eq!(v.value(2), 3);
    let back = d.join("back.csv");
    convert(&pq, &back, &opts(), &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&back).unwrap(), "v\n1\n\n3\n");
    assert_eq!(convert(&back, &d.join("again.parquet"), &opts(), &NoHooks).unwrap().rows, 3);
    assert_eq!(count(&back, &opts()), 3);
}

#[test]
fn count_matches_conversion() {
    let d = dir("count");
    let src = d.join("c.csv");
    std::fs::write(&src, "a,b\n1,2\n\n3,4\n\n").unwrap();
    assert_eq!(count(&src, &opts()), 2);
    assert_eq!(convert(&src, &d.join("c.parquet"), &opts(), &NoHooks).unwrap().rows, 2);
    std::fs::write(&src, "report\n\na,b\n1,2\n3,4\n").unwrap();
    assert_eq!(count(&src, &opts()), 2);
    assert_eq!(convert(&src, &d.join("c.parquet"), &opts(), &NoHooks).unwrap().rows, 2);
}

#[test]
fn split_is_atomic_across_restarts() {
    // Invalid UTF-8 at the very end restarts the job after parts were written.
    let d = dir("split-restart");
    let src = d.join("late.csv");
    let mut bytes = b"city,n\n".to_vec();
    for i in 0..300_000 {
        bytes.extend_from_slice(format!("Paris,{i}\n").as_bytes());
    }
    bytes.extend_from_slice(b"Z\xfcrich,1\n");
    std::fs::write(&src, &bytes).unwrap();
    let out = d.join("parts");
    let o = ConvertOptions { split_rows: Some(50_000), to: Some("parquet".into()), ..Default::default() };
    let r = convert(&src, &out, &o, &NoHooks).unwrap();
    assert!(r.restarts >= 1);
    assert_eq!(r.rows, 300_001);
    let expected: Vec<String> = (0..7).map(|i| format!("part-{i:05}.parquet")).collect();
    let listing = || {
        let mut names: Vec<String> =
            std::fs::read_dir(&out).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        names.sort();
        names
    };
    assert_eq!(listing(), expected, "only the published parts remain");
    assert!(r.parts.iter().all(|p| Path::new(&p.path).exists()));
    let last = read_parquet(Path::new(&r.parts[6].path));
    assert_eq!(last.column(0).as_string::<i32>().value(0), "Zürich");

    // a second run refuses to replace parts before doing any work
    let err = convert(&src, &out, &o, &NoHooks).unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    assert_eq!(listing(), expected);

    // replacing them works, and parts left over from a longer run are reported
    std::fs::write(out.join("part-00007.parquet"), b"old").unwrap();
    let r = convert(&src, &out, &ConvertOptions { overwrite: true, ..o.clone() }, &NoHooks).unwrap();
    assert_eq!(r.parts.len(), 7);
    assert!(r.warnings.iter().any(|w| w.contains("older part")), "{:?}", r.warnings);

    // a failed split leaves no directory behind
    let missing = d.join("never");
    let bad = ConvertOptions { columns: Some(vec!["nope".into()]), ..o };
    assert!(convert(&src, &missing, &bad, &NoHooks).is_err());
    assert!(!missing.exists());
}

#[test]
fn dialect_options_must_be_ascii() {
    let d = dir("ascii");
    let src = d.join("in.csv");
    std::fs::write(&src, "a\u{a6}b\n1\u{a6}2\n").unwrap();
    let o = ConvertOptions { sniff: SniffOptions { delimiter: Some("\u{a6}".into()), ..Default::default() }, ..opts() };
    let err = convert(&src, &d.join("o.parquet"), &o, &NoHooks).unwrap_err();
    assert!(err.to_string().contains("single ASCII character"), "{err}");
    let o = ConvertOptions { sniff: SniffOptions { quote: Some("\u{fe}".into()), ..Default::default() }, ..opts() };
    assert!(convert(&src, &d.join("o.parquet"), &o, &NoHooks).is_err());
    let o = ConvertOptions { sniff: SniffOptions { delimiter: Some("||".into()), ..Default::default() }, ..opts() };
    assert!(convert(&src, &d.join("o.parquet"), &o, &NoHooks).is_err());
}

#[test]
fn dcz_to_csv_honours_output_options() {
    let d = dir("dczopt");
    let src = d.join("in.csv");
    std::fs::write(&src, "a,b\n1,x\n2,y\n").unwrap();
    let packed = d.join("in.dcz");
    convert(&src, &packed, &opts(), &NoHooks).unwrap();
    let out = d.join("out.csv");
    convert(&packed, &out, &ConvertOptions { out_crlf: true, ..opts() }, &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "a,b\r\n1,x\r\n2,y\r\n");
    convert(&packed, &out, &ConvertOptions { out_header: Some(false), ..opts() }, &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "1,x\n2,y\n");
}

#[test]
fn dcz_blocks_stay_bounded_without_line_feeds() {
    let d = dir("cr");
    let block = 256usize << 10;
    // CR-only line endings
    let src = d.join("cr.csv");
    let mut s = String::from("id,v\r");
    for i in 0..300_000 {
        s.push_str(&format!("{i},{}\r", i % 97));
    }
    // and one long line without any terminator
    let flat = d.join("flat.csv");
    let line: String = (0..400_000).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
    std::fs::write(&src, &s).unwrap();
    std::fs::write(&flat, &line).unwrap();
    for (path, text) in [(&src, s.as_str()), (&flat, line.as_str())] {
        let packed = path.with_extension("dcz");
        convert(path, &packed, &ConvertOptions { chunk_size: Some(block), ..opts() }, &NoHooks).unwrap();
        let footer = dcz::read_footer(&packed).unwrap();
        assert!(footer.blocks.len() > 3, "{}: {} blocks", path.display(), footer.blocks.len());
        assert!(footer.blocks.iter().all(|b| b.raw_len as usize <= 2 * block), "{}", path.display());
        let back = path.with_extension("back");
        dcz::decompress_file(&packed, &back, true, 2, &NoHooks).unwrap();
        assert_eq!(std::fs::read(&back).unwrap(), text.as_bytes());
    }
}

/// xorshift: deterministic pseudo-random bytes for the corruption tests.
fn rng(mut x: u64) -> impl FnMut() -> u64 {
    move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}

fn sample_csv(rows: usize) -> String {
    let mut csv = String::from("id,price,city,when,note\n");
    for i in 0..rows {
        csv.push_str(&format!(
            "{i},{}.{:02},{},2024-01-{:02}T10:00:00,{}\n",
            i % 500,
            i % 100,
            ["Pune", "Rome", "\"Oslo, NO\""][i % 3],
            i % 28 + 1,
            if i % 5 == 0 { "" } else { "x" }
        ));
    }
    csv
}

#[test]
fn corrupt_dcz_never_panics() {
    for rows in [3, 40, 3000] {
        let csv = sample_csv(rows);
        let packed = dcz::compress_bytes(csv.as_bytes(), true, Profile::Balanced).unwrap();
        assert_eq!(dcz::decompress_bytes(&packed).unwrap(), csv.as_bytes());
        let block = dcz::block::encode(csv.as_bytes(), Dialect::default(), true, Profile::Fast).unwrap().unwrap();
        let mut rnd = rng(0x2545_F491_4F6C_DD1D ^ rows as u64);
        for _ in 0..4000 {
            let mut bad = packed.clone();
            let mut payload = block.payload.clone();
            for _ in 0..rnd() % 4 + 1 {
                let i = rnd() as usize % bad.len();
                bad[i] = rnd() as u8;
                let j = rnd() as usize % payload.len();
                payload[j] = rnd() as u8;
            }
            let decoded = std::panic::catch_unwind(|| dcz::decompress_bytes(&bad)).expect("decompress_bytes panicked");
            if let Ok(out) = decoded {
                assert_eq!(out, csv.as_bytes(), "corruption went unnoticed");
            }
            std::panic::catch_unwind(|| dcz::block::decode(&payload, csv.len())).expect("block decode panicked").ok();
        }
    }
}

#[test]
fn corrupt_packed_values_never_panic() {
    use dataconv_core::pack::{pack, unpack, Packed, Values};
    let inputs = [
        Values::Int((0..500).map(|i| i * 7 - 300).collect()),
        Values::Float((0..500).map(|i| i as f64 / 8.0).collect()),
        Values::Str((0..500).map(|i| format!("k{}", i % 9).into_bytes()).collect()),
        Values::Bool((0..500).map(|i| i % 3 == 0).collect()),
    ];
    for values in inputs {
        let p = Packed { values, valid: None, tag: b"t".to_vec() };
        let buf = pack(&p, Profile::Fast).unwrap();
        // the frame checksums its payload, so fix the checksum after each change to
        // reach the decoder: header(16) kind(1) len(varint) crc(4) payload
        let mut len_bytes = 0;
        while buf[17 + len_bytes] & 0x80 != 0 {
            len_bytes += 1;
        }
        let crc_at = 17 + len_bytes + 1;
        let mut rnd = rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..3000 {
            let mut bad = buf.clone();
            for _ in 0..rnd() % 3 + 1 {
                let i = crc_at + 4 + rnd() as usize % (bad.len() - crc_at - 4);
                bad[i] = rnd() as u8;
            }
            let crc = crc32fast::hash(&bad[crc_at + 4..]);
            bad[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
            std::panic::catch_unwind(|| unpack(&bad)).expect("unpack panicked").ok();
        }
    }
}
