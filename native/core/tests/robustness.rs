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
        dcz::decompress_file(&packed, &back, true, 2, None, &NoHooks).unwrap();
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

#[test]
fn a_stray_quote_costs_one_row() {
    let d = dir("stray");
    let src = d.join("stray.csv");
    let mut s = String::from("id,name,note\n");
    for i in 0..200_000 {
        match i {
            // a quote inside a field is a literal character
            5 => s.push_str("5,55\" screen,x\n"),
            // a quote that is never closed costs its own line only
            10 => s.push_str("10,name10,\"note10\n"),
            _ => s.push_str(&format!("{i},name{i},note{i}\n")),
        }
    }
    std::fs::write(&src, &s).unwrap();
    let dst = d.join("stray.parquet");
    let o = ConvertOptions { chunk_size: Some(256 << 10), ..opts() };
    let r = convert(&src, &dst, &o, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (199_999, 1), "{:?}", r.warnings);
    let rejected = std::fs::read_to_string(r.quarantine.unwrap()).unwrap();
    assert_eq!(rejected, "id,name,note\n10,name10,\"note10\n");
    let b = read_parquet(&dst);
    assert_eq!(b.column(1).as_string::<i32>().value(5), "55\" screen");
    assert_eq!(b.column(0).as_primitive::<Int64Type>().value(10), 11);
    assert_eq!(count(&src, &o), 200_000);

    // a later quote with text after it would close the first one across lines: that
    // marks the first as a stray, which still costs its own line only
    s = s.replace("15,name15,note15\n", "15,name15,note\"15\n");
    s = s.replace("250,name250,note250\n", "250,\"Smith250, John\",note250\n");
    std::fs::write(&src, &s).unwrap();
    let r = convert(&src, &dst, &o, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (199_999, 1), "{:?}", r.warnings);
    let rejected = std::fs::read_to_string(r.quarantine.unwrap()).unwrap();
    assert_eq!(rejected, "id,name,note\n10,name10,\"note10\n");
    let b = read_parquet(&dst);
    assert_eq!(b.column(2).as_string::<i32>().value(14), "note\"15");
    assert_eq!(count(&src, &o), 200_000);
    // so does a stray quote every 1000 rows, each pairing with the next
    let mut s = String::from("id,name,note\n");
    for i in 0..100_000 {
        if i % 1000 == 500 {
            s.push_str(&format!("{i},\"name{i},note{i}\n"));
        } else {
            s.push_str(&format!("{i},name{i},note{i}\n"));
        }
    }
    std::fs::write(&src, &s).unwrap();
    let r = convert(&src, &dst, &o, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (99_900, 100), "{:?}", r.warnings);
    assert_eq!(count(&src, &o), 100_000);
    // a later quote followed by a delimiter or line break closes the first one: then
    // the record between them is one malformed row, and the warning says how many
    // lines it holds
    let mut s = String::from("id,name,v\n");
    for i in 0..20 {
        match i {
            3 => s.push_str("3,\"name3,3\n"),
            8 => s.push_str("8,name8,8\"\n"),
            _ => s.push_str(&format!("{i},name{i},{i}\n")),
        }
    }
    std::fs::write(&src, &s).unwrap();
    let r = convert(&src, &dst, &o, &NoHooks).unwrap();
    assert_eq!((r.rows, r.bad_rows), (14, 1), "{:?}", r.warnings);
    assert!(r.warnings.iter().any(|w| w.contains("1 malformed rows (6 lines)")), "{:?}", r.warnings);
}

#[test]
fn jsonl_round_trips_losslessly() {
    let d = dir("jsonl");
    let src = d.join("in.csv");
    std::fs::write(&src, "id,amount,ratio,name\n18446744073709551615,1.00000000000000000001,NaN,a\n1,2.5,0.5,b\n")
        .unwrap();
    let jl = d.join("mid.jsonl");
    convert(&src, &jl, &opts(), &NoHooks).unwrap();
    let text = std::fs::read_to_string(&jl).unwrap();
    assert!(text.contains("\"ratio\":\"NaN\""), "{text}");
    let r = convert(&jl, &d.join("back.parquet"), &opts(), &NoHooks).unwrap();
    let types: Vec<&str> = r.schema.iter().map(|(_, t)| t.as_str()).collect();
    assert_eq!(types, ["decimal(20,0)", "decimal(21,20)", "float64", "string"], "{:?}", r.schema);
    let back = d.join("back.csv");
    convert(&d.join("back.parquet"), &back, &opts(), &NoHooks).unwrap();
    let text = std::fs::read_to_string(&back).unwrap();
    assert!(text.starts_with("id,amount,ratio,name\n18446744073709551615,1.00000000000000000001,NaN,a\n"), "{text}");
}

#[test]
fn json_kept_as_text_round_trips_as_json() {
    let d = dir("json-text");
    let src = d.join("in.jsonl");
    std::fs::write(
        &src,
        concat!(
            "{\"v\": 1e400, \"m\": 1, \"n\": [1, 2], \"l\": [1, \"a\", {}], \"s\": {\"x\": 1}, \"p\": [1e400, 2]}\n",
            "{\"v\": 1.5, \"m\": \"1\", \"n\": null, \"l\": [null, true], \"s\": {\"x\": \"1\"}, \"p\": null}\n",
        ),
    )
    .unwrap();
    for mid in ["mid.parquet", "mid.arrow"] {
        let mid = d.join(mid);
        convert(&src, &mid, &opts(), &NoHooks).unwrap();
        let back = d.join("back.jsonl");
        convert(&mid, &back, &opts(), &NoHooks).unwrap();
        assert_eq!(
            std::fs::read_to_string(&back).unwrap(),
            concat!(
                "{\"v\":1e+400,\"m\":1,\"n\":[1,2],\"l\":[1,\"a\",{}],\"s\":{\"x\":1},\"p\":[1e+400,2]}\n",
                "{\"v\":1.5,\"m\":\"1\",\"n\":null,\"l\":[null,true],\"s\":{\"x\":\"1\"},\"p\":null}\n",
            ),
            "{}",
            mid.display()
        );
        // a CSV cell holding a list or struct is its JSON, marked items included
        let csv = d.join("back.csv");
        convert(&mid, &csv, &opts(), &NoHooks).unwrap();
        assert_eq!(
            std::fs::read_to_string(&csv).unwrap(),
            concat!(
                "v,m,n,l,s,p\n",
                "1e+400,1,\"[1,2]\",\"[1,\"\"a\"\",{}]\",\"{\"\"x\"\":1}\",\"[1e+400,2]\"\n",
                "1.5,\"\"\"1\"\"\",,\"[null,true]\",\"{\"\"x\"\":\"\"1\"\"}\",\n",
            ),
            "{}",
            mid.display()
        );
    }
}

#[test]
fn foreign_types_are_written_like_their_text() {
    use arrow::array::{ArrayRef, Date64Array, Int64Builder, MapBuilder, NullArray, StringBuilder};
    use arrow::datatypes::{DataType, Field};
    use std::collections::HashMap;
    use std::sync::Arc;

    let d = dir("foreign-types");
    // a map with a repeated key and a null value
    let mut m = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    m.keys().append_value("a");
    m.values().append_value(1);
    m.keys().append_value("a");
    m.values().append_null();
    m.append(true).unwrap();
    m.append(false).unwrap();
    m.append(true).unwrap();
    // keys that are not text
    let mut k = MapBuilder::new(None, Int64Builder::new(), StringBuilder::new());
    for (key, v) in [(1, "x"), (2, "y")] {
        k.keys().append_value(key);
        k.values().append_value(v);
    }
    k.append(true).unwrap();
    k.append(false).unwrap();
    k.append(true).unwrap();
    // values that hold JSON
    let marked = HashMap::from([("dataconv.json".to_string(), "value".to_string())]);
    let mut j = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_values_field(Field::new("values", DataType::Utf8, true).with_metadata(marked));
    j.keys().append_value("v");
    j.values().append_value("[1,2]");
    j.append(true).unwrap();
    j.append(false).unwrap();
    j.append(true).unwrap();
    let day = 19_724 * 86_400_000; // 2024-01-02
    let cols: Vec<(&str, ArrayRef)> = vec![
        ("n", Arc::new(NullArray::new(3))),
        ("d", Arc::new(Date64Array::from(vec![Some(day), Some(day + 11_045_006), None]))),
        ("m", Arc::new(m.finish())),
        ("k", Arc::new(k.finish())),
        ("j", Arc::new(j.finish())),
    ];
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let src = d.join("foreign.arrow");
    let mut w = arrow::ipc::writer::FileWriter::try_new(File::create(&src).unwrap(), &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    drop(w);

    let out = d.join("foreign.jsonl");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        concat!(
            "{\"n\":null,\"d\":\"2024-01-02\",\"m\":{\"a\":1,\"a\":null},\"k\":{\"1\":\"x\",\"2\":\"y\"},\"j\":{\"v\":[1,2]}}\n",
            "{\"n\":null,\"d\":\"2024-01-02T03:04:05.006\",\"m\":null,\"k\":null,\"j\":null}\n",
            "{\"n\":null,\"d\":null,\"m\":{},\"k\":{},\"j\":{}}\n",
        )
    );
    // a null is an empty unquoted field, whatever the column's type
    let out = d.join("foreign.csv");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        concat!(
            "n,d,m,k,j\n",
            ",2024-01-02,\"{\"\"a\"\":1,\"\"a\"\":null}\",\"{\"\"1\"\":\"\"x\"\",\"\"2\"\":\"\"y\"\"}\",\"{\"\"v\"\":[1,2]}\"\n",
            ",2024-01-02T03:04:05.006,,,\n",
            ",,{},{},{}\n",
        )
    );
    // head shows the values as CSV output writes them, and the types by their names
    let p = dataconv_core::inspect::head(&src, 3, &opts()).unwrap();
    assert_eq!(p.types, ["null", "date64", "map<string, int64>", "map<int64, string>", "map<string, string>"]);
    let some = |s: &str| Some(s.to_string());
    assert_eq!(
        p.rows[0],
        [
            None,
            some("2024-01-02"),
            some("{\"a\":1,\"a\":null}"),
            some("{\"1\":\"x\",\"2\":\"y\"}"),
            some("{\"v\":[1,2]}")
        ]
    );
    assert_eq!(p.rows[2], [None, None, some("{}"), some("{}"), some("{}")]);
}

#[test]
fn nulls_among_dictionary_values_stay_null() {
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Int32Type};
    use std::sync::Arc;

    let d = dir("dictionary-nulls");
    // pyarrow's dictionary_encode(null_encoding="encode") stores the null as a value
    let values = Arc::new(StringArray::from(vec![Some("a"), Some(""), None]));
    let dict = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 2, 2]), values).unwrap();
    let item = Arc::new(Field::new("item", dict.data_type().clone(), true));
    let list = ListArray::new(item, OffsetBuffer::new(vec![0, 3, 3, 4, 4].into()), Arc::new(dict.clone()), None);
    let cols: Vec<(&str, ArrayRef)> = vec![("d", Arc::new(dict)), ("l", Arc::new(list))];
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let src = d.join("dict.arrow");
    let mut w = arrow::ipc::writer::FileWriter::try_new(File::create(&src).unwrap(), &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    drop(w);
    let out = d.join("dict.jsonl");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        concat!(
            "{\"d\":\"a\",\"l\":[\"a\",\"\",null]}\n",
            "{\"d\":\"\",\"l\":[]}\n",
            "{\"d\":null,\"l\":[null]}\n",
            "{\"d\":null,\"l\":[]}\n",
        )
    );
    let out = d.join("dict.csv");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "d,l\na,\"[\"\"a\"\",\"\"\"\",null]\"\n\"\",[]\n,[null]\n,[]\n");
}

#[test]
fn half_floats_read_back_exactly() {
    use arrow::array::Float32Array;
    use arrow::datatypes::DataType;

    let d = dir("float16");
    let singles = Float32Array::from(vec![Some(1.0), Some(6e-8), Some(f32::NAN), None]);
    let h = arrow::compute::cast(&singles, &DataType::Float16).unwrap();
    let batch = RecordBatch::try_from_iter([("h", h)]).unwrap();
    let src = d.join("half.arrow");
    let mut w = arrow::ipc::writer::FileWriter::try_new(File::create(&src).unwrap(), &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    drop(w);
    // written as the float32 each widens to, alike in CSV and JSON
    let out = d.join("half.csv");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "h\n1.0\n5.9604645e-8\nNaN\n\n");
    let out = d.join("half.jsonl");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "{\"h\":1.0}\n{\"h\":5.9604645e-8}\n{\"h\":\"NaN\"}\n{\"h\":null}\n"
    );
}

#[test]
fn parquet_info_names_codecs_and_logical_types() {
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let d = dir("parquet-info");
    let cols: Vec<(&str, ArrayRef)> = vec![
        ("s", Arc::new(StringArray::from(vec!["x"]))),
        ("u", Arc::new(UInt32Array::from(vec![1]))),
        ("t", Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC"))),
        ("i", Arc::new(Int64Array::from(vec![1]))),
    ];
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let src = d.join("t.parquet");
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap())).build();
    let mut w = ArrowWriter::try_new(File::create(&src).unwrap(), batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let info = dataconv_core::inspect::info(&src, false, &opts()).unwrap();
    let columns = info["columns"].as_array().unwrap();
    let logical: Vec<&serde_json::Value> = columns.iter().map(|c| &c["logical_type"]).collect();
    assert_eq!(
        logical,
        [&serde_json::json!("string"), &"uint32".into(), &"timestamp[ms,utc]".into(), &serde_json::Value::Null]
    );
    // a file does not store the level it was compressed at
    assert_eq!(columns[0]["compression"], serde_json::json!(["zstd"]));
}

#[test]
fn stored_time_zones_survive_a_coarser_parquet_unit() {
    use arrow::array::{ArrayRef, StructArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::{encode_arrow_schema, ArrowWriter, ARROW_SCHEMA_META_KEY};
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let d = dir("stored-tz");
    // 2024-01-02T03:04:05+05:30
    let t: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_704_144_845_000]).with_timezone("+05:30"));
    let st: ArrayRef =
        Arc::new(StructArray::from(vec![(Arc::new(Field::new("t", t.data_type().clone(), true)), t.clone())]));
    let batch = RecordBatch::try_from_iter([("t", t), ("st", st)]).unwrap();
    // what pyarrow stores for timestamp[s, tz]: seconds in the Arrow schema, milliseconds
    // in Parquet
    let secs = DataType::Timestamp(TimeUnit::Second, Some("+05:30".into()));
    let stored = Schema::new(vec![
        Field::new("t", secs.clone(), true),
        Field::new("st", DataType::Struct(vec![Field::new("t", secs, true)].into()), true),
    ]);
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            encode_arrow_schema(&stored),
        )]))
        .build();
    let src = d.join("tz.parquet");
    let options = ArrowWriterOptions::new().with_properties(props).with_skip_arrow_metadata(true);
    let mut w = ArrowWriter::try_new_with_options(File::create(&src).unwrap(), batch.schema(), options).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let out = d.join("tz.csv");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "t,st\n2024-01-02T03:04:05+05:30,\"{\"\"t\"\":\"\"2024-01-02T03:04:05+05:30\"\"}\"\n"
    );
}

#[test]
fn json_output_writes_times_like_csv() {
    let d = dir("json-times");
    let src = d.join("t.csv");
    std::fs::write(&src, "t,u,d\n2024-01-02T03:04:05.5+05:30,2024-01-02 03:04:05,2024-01-02\n").unwrap();
    let out = d.join("t.jsonl");
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "{\"t\":\"2024-01-02T03:04:05.5+05:30\",\"u\":\"2024-01-02T03:04:05\",\"d\":\"2024-01-02\"}\n"
    );
}

#[test]
fn jsonl_types_widen_beyond_the_sample() {
    let d = dir("jsonl-replan");
    let src = d.join("late.jsonl");
    let mut s = String::from("\u{feff}");
    for i in 0..20_000 {
        s.push_str(&format!("{{\"z\": {i}, \"a\": \"x{i}\"}}\n"));
    }
    s.push_str("{\"z\": 1.5, \"a\": 7, \"new\": true}\n");
    std::fs::write(&src, &s).unwrap();
    let pq = d.join("late.parquet");
    let r = convert(&src, &pq, &ConvertOptions { sample_bytes: Some(64 << 10), ..opts() }, &NoHooks).unwrap();
    assert_eq!((r.restarts, r.rows), (1, 20_001), "{:?}", r.warnings);
    let names: Vec<&str> = r.schema.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["z", "a", "new"]);
    assert_eq!((r.schema[0].1.as_str(), r.schema[1].1.as_str()), ("float64", "string"));
    assert!(r.warnings.iter().any(|w| w.contains("'a' mixes JSON types")), "{:?}", r.warnings);
    let b = read_parquet(&pq);
    let a = b.column(1).as_string::<i32>();
    assert_eq!((a.value(0), a.value(20_000)), ("\"x0\"", "7"));
}

#[test]
fn quarantine_never_replaces_files_by_accident() {
    let d = dir("quarantine");
    let src = d.join("ragged.csv");
    std::fs::write(&src, "a,b\n1,2\n3\n4,5\n").unwrap();
    let out = d.join("out.jsonl");
    let q = d.join("out.jsonl.rejected.csv");
    std::fs::write(&q, "keep me").unwrap();
    let err = convert(&src, &out, &ConvertOptions::default(), &NoHooks).unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    assert_eq!(std::fs::read_to_string(&q).unwrap(), "keep me");
    assert!(!out.exists());
    let o = ConvertOptions { quarantine: Some(src.display().to_string()), ..opts() };
    assert!(convert(&src, &out, &o, &NoHooks).is_err());
    assert_eq!(std::fs::read_to_string(&src).unwrap(), "a,b\n1,2\n3\n4,5\n");
    let r = convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(r.bad_rows, 1);
    assert_eq!(std::fs::read_to_string(&q).unwrap(), "a,b\n3\n");
    std::fs::write(&src, "a,b\n1,2\n").unwrap();
    convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert!(!q.exists(), "a clean re-run removes rejected rows of an earlier run");
}

#[test]
fn compress_keeps_compressed_files_byte_for_byte() {
    use std::io::Write;
    let d = dir("archive");
    let gz = d.join("data.csv.gz");
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(sample_csv(2000).as_bytes()).unwrap();
    let bytes = enc.finish().unwrap();
    std::fs::write(&gz, &bytes).unwrap();
    let packed = d.join("data.csv.gz.dcz");
    let eo = dcz::EncodeOptions { profile: Profile::Balanced, threads: 2, block_size: None, memory: None };
    dcz::compress_file(&gz, &packed, &eo, true, &NoHooks).unwrap();
    let back = d.join("restored.csv.gz");
    dcz::decompress_file(&packed, &back, true, 2, None, &NoHooks).unwrap();
    assert_eq!(std::fs::read(&back).unwrap(), bytes);
    // converting the same file to DCZ stores the CSV itself
    let conv = d.join("conv.dcz");
    convert(&gz, &conv, &opts(), &NoHooks).unwrap();
    let csv = d.join("conv.csv");
    convert(&conv, &csv, &opts(), &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&csv).unwrap(), sample_csv(2000));
}

#[test]
fn concatenated_lz4_frames_are_read_fully() {
    use std::io::Write;
    let d = dir("lz4");
    let rows = |a: usize, b: usize| (a..b).map(|i| format!("{i},v{i}\n")).collect::<String>();
    let mut bytes = Vec::new();
    for text in [format!("id,v\n{}", rows(0, 1000)), rows(1000, 2000)] {
        let mut e = lz4_flex::frame::FrameEncoder::new(Vec::new());
        e.write_all(text.as_bytes()).unwrap();
        bytes.extend_from_slice(&e.finish().unwrap());
    }
    let src = d.join("two.csv.lz4");
    std::fs::write(&src, &bytes).unwrap();
    assert_eq!(count(&src, &opts()), 2000);
    assert_eq!(convert(&src, &d.join("two.parquet"), &opts(), &NoHooks).unwrap().rows, 2000);
}

#[test]
fn latin1_means_latin1() {
    let d = dir("latin1");
    let src = d.join("l1.csv");
    std::fs::write(&src, b"name,n\n\x80x,1\nZ\xfcrich,2\n").unwrap();
    let o = ConvertOptions { sniff: SniffOptions { encoding: Some("latin-1".into()), ..Default::default() }, ..opts() };
    let pq = d.join("l1.parquet");
    convert(&src, &pq, &o, &NoHooks).unwrap();
    let b = read_parquet(&pq);
    let name = b.column(0).as_string::<i32>();
    assert_eq!((name.value(0), name.value(1)), ("\u{80}x", "Zürich"));
}

#[test]
fn second_timestamps_and_nested_values_stay_readable() {
    use arrow::datatypes::{DataType, TimeUnit};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let d = dir("types");
    let src = d.join("ts.csv");
    std::fs::write(&src, "t,n\n2024-01-01 10:00:00,1\n2024-01-02 11:30:15,2\n").unwrap();
    let pq = d.join("ts.parquet");
    let r = convert(&src, &pq, &opts(), &NoHooks).unwrap();
    assert_eq!(r.schema[0].1, "timestamp[s]");
    // stored as a Parquet timestamp (milliseconds) rather than a bare integer
    let reader = SerializedFileReader::new(File::open(&pq).unwrap()).unwrap();
    let col = reader.metadata().file_metadata().schema_descr().column(0);
    assert!(matches!(col.logical_type_ref(), Some(parquet::basic::LogicalType::Timestamp { .. })));
    assert_eq!(read_parquet(&pq).column(0).data_type(), &DataType::Timestamp(TimeUnit::Millisecond, None));
    let back = d.join("ts-back.csv");
    convert(&pq, &back, &opts(), &NoHooks).unwrap();
    assert_eq!(std::fs::read_to_string(&back).unwrap(), "t,n\n2024-01-01T10:00:00,1\n2024-01-02T11:30:15,2\n");
    // nested JSON values are written to CSV as JSON text
    let jl = d.join("nested.jsonl");
    std::fs::write(&jl, "{\"id\": 1, \"p\": {\"x\": null, \"y\": \"b\"}, \"tags\": [\"a\", \"b\"]}\n").unwrap();
    let out = d.join("nested.csv");
    convert(&jl, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "id,p,tags\n1,\"{\"\"x\"\":null,\"\"y\"\":\"\"b\"\"}\",\"[\"\"a\"\",\"\"b\"\"]\"\n"
    );
}

#[test]
fn a_directory_is_not_a_file() {
    let d = dir("isdir");
    let err = convert(&d, &d.join("x.parquet"), &opts(), &NoHooks).unwrap_err();
    assert!(err.to_string().contains("is a directory"), "{err}");
}

#[test]
fn arrow_ipc_streams_are_read() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let d = dir("ipc-stream");
    let src = d.join("stream.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(File::create(&src).unwrap(), &schema).unwrap();
    w.write(&batch).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    drop(w);
    assert_eq!(count(&src, &opts()), 6);
    let out = d.join("stream.csv");
    assert_eq!(convert(&src, &out, &opts(), &NoHooks).unwrap().rows, 6);
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "x\n1\n2\n3\n1\n2\n3\n");
}

#[test]
fn a_stray_quote_in_the_first_record_does_not_hide_the_types() {
    let d = dir("stray-first");
    let src = d.join("in.csv");
    std::fs::write(&src, "a,b\n1,\"open\n2,x\n3,y\n").unwrap();
    let out = d.join("out.jsonl");
    let r = convert(&src, &out, &opts(), &NoHooks).unwrap();
    assert_eq!(r.bad_rows, 1);
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "{\"a\":2,\"b\":\"x\"}\n{\"a\":3,\"b\":\"y\"}\n");
}

#[test]
fn head_of_a_file_without_rows_names_its_columns() {
    let d = dir("header-only");
    let src = d.join("h.csv");
    std::fs::write(&src, "a,b,c\n").unwrap();
    let p = dataconv_core::inspect::head(&src, 5, &opts()).unwrap();
    assert_eq!(p.columns, ["a", "b", "c"]);
    assert!(p.rows.is_empty());
}

#[test]
fn json_nested_too_deep_is_named_as_such() {
    let d = dir("too-deep");
    let src = d.join("deep.jsonl");
    std::fs::write(&src, format!("{{\"a\":{}{}}}\n", "[".repeat(200), "]".repeat(200))).unwrap();
    let e = convert(&src, &d.join("deep.csv"), &opts(), &NoHooks).unwrap_err().to_string();
    assert!(e.contains("nested deeper than 128 levels"), "{e}");
    assert!(!d.join("deep.csv").exists());
}

#[test]
fn options_that_cannot_work_are_refused() {
    let d = dir("refused");
    let src = d.join("in.csv");
    std::fs::write(&src, "a,b\n1,2\n").unwrap();
    let out = d.join("out.parquet");
    let sniff = |s: SniffOptions| ConvertOptions { sniff: s, ..opts() };
    let err = |o: &ConvertOptions| convert(&src, &out, o, &NoHooks).unwrap_err().to_string();
    assert!(err(&sniff(SniffOptions { encoding: Some("bogus-enc".into()), ..Default::default() }))
        .contains("unknown encoding"));
    assert!(err(&sniff(SniffOptions { delimiter: Some("\"".into()), ..Default::default() })).contains("must differ"));
    // a line break cannot separate fields or quote them
    for (delimiter, quote) in [(Some("\\n"), None), (Some("\\r"), None), (Some("\r"), None), (None, Some("\n"))] {
        let o = sniff(SniffOptions {
            delimiter: delimiter.map(Into::into),
            quote: quote.map(Into::into),
            ..Default::default()
        });
        assert!(err(&o).contains("line break"), "{delimiter:?} {quote:?}");
    }
    // nor can output fields be separated by what quotes or ends them
    let csv = d.join("out.csv");
    for c in ["\"", "\\n", "\r"] {
        let o = ConvertOptions { out_delimiter: Some(c.into()), ..opts() };
        let e = convert(&src, &csv, &o, &NoHooks).unwrap_err().to_string();
        assert!(e.contains("cannot be the output delimiter"), "{c:?}: {e}");
        assert!(!csv.exists());
    }
    // Python spells encodings with underscores; they are understood
    let o = sniff(SniffOptions { encoding: Some("utf_8".into()), ..Default::default() });
    assert_eq!(convert(&src, &out, &o, &NoHooks).unwrap().rows, 1);
    // split parts are named by the pattern, never placed elsewhere
    let o = ConvertOptions { split_rows: Some(1), split_pattern: Some("../x-{n}".into()), ..opts() };
    assert!(convert(&src, &d.join("parts"), &o, &NoHooks).unwrap_err().to_string().contains("file name"));
    assert!(!d.join("x-0.csv").exists());
    // every command names a directory input as such
    for e in [dataconv_core::inspect::count(&d, &opts()).unwrap_err(), dcz::read_footer(&d).unwrap_err()] {
        assert!(e.to_string().contains("is a directory"), "{e}");
    }
}
