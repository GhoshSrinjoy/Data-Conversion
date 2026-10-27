//! Standalone `dataconv` binary: the native engine without Python.
//!
//! Supports the core commands of the Python CLI (convert, compress, decompress,
//! split, info, head, count, sniff, doctor) with the same option names.

use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;
use std::time::Instant;

use dataconv_core::codec::Profile;
use dataconv_core::convert::{self, BadLines, ConvertOptions, Hooks, InferMode, Report};
use dataconv_core::types::TypesMode;
use dataconv_core::{dcz, inspect, resources, Error};

const USAGE: &str = "dataconv (native) - fast, lossless data conversion and compression

usage:
  dataconv convert SRC DST [options]     CSV/TSV, JSON Lines, Parquet, Arrow IPC, DCZ
  dataconv compress SRC [DST] [--profile fast|balanced|max]
  dataconv decompress SRC [DST]
  dataconv split SRC DIR (--rows N | --size 100MB) [--to FORMAT] [--pattern part-{n:05}]
  dataconv info FILE [--exact]           JSON description of a file
  dataconv head FILE [-n 10]
  dataconv count FILE
  dataconv sniff FILE
  dataconv doctor                        resources the engine will use

options:
  --to FORMAT            output format when the extension does not say
  --profile P            fast | balanced (default) | max
  --compression C        zstd | snappy | lz4 | gzip | brotli | none
  --level N              codec level
  --types T              infer (lossless, default) | string | loose
  --schema SPEC          type overrides, e.g. \"zip=string,amount=decimal(12,2)\"
  --delimiter D          input delimiter (default: detected; \\t for tab)
  --encoding E           input encoding (default: detected)
  --no-header            the first row is data
  --bad-lines B          quarantine (default) | skip | error | pad
  --columns a,b,c        columns to keep
  --limit N              at most N rows
  --infer full           infer types from the whole file
  --threads N            worker threads (default: all usable CPUs)
  --memory SIZE          memory budget, e.g. 2GB (default: half the available RAM)
  -f, --overwrite        replace existing outputs
  -q, --quiet            no progress or summary
  --json                 print the report as JSON
";

struct Args {
    positional: Vec<String>,
    flags: HashMap<String, Option<String>>,
}

const SWITCHES: &[&str] = &["overwrite", "f", "quiet", "q", "json", "no-header", "header", "exact", "crlf", "help", "h"];

fn parse_args(raw: &[String]) -> Args {
    let mut positional = Vec::new();
    let mut flags = HashMap::new();
    let mut i = 0;
    while i < raw.len() {
        let a = &raw[i];
        if let Some(name) = a.strip_prefix("--").or_else(|| a.strip_prefix('-').filter(|n| n.len() == 1)) {
            if let Some((k, v)) = name.split_once('=') {
                flags.insert(k.to_string(), Some(v.to_string()));
            } else if SWITCHES.contains(&name) {
                flags.insert(name.to_string(), None);
            } else {
                flags.insert(name.to_string(), raw.get(i + 1).cloned());
                i += 1;
            }
        } else {
            positional.push(a.clone());
        }
        i += 1;
    }
    Args { positional, flags }
}

impl Args {
    fn get(&self, name: &str) -> Option<&str> {
        self.flags.get(name).and_then(|v| v.as_deref())
    }

    fn has(&self, name: &str) -> bool {
        self.flags.contains_key(name)
    }

    fn num<T: std::str::FromStr>(&self, name: &str) -> Result<Option<T>, String> {
        match self.get(name) {
            None => Ok(None),
            Some(v) => v.parse().map(Some).map_err(|_| format!("--{name} expects a number, got '{v}'")),
        }
    }
}

fn parse_size(text: &str, default_unit: u64) -> Result<u64, String> {
    let t = text.trim().to_ascii_lowercase();
    let digits: String = t.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
    let unit = t[digits.len()..].trim().trim_end_matches('b').trim_end_matches('i');
    let n: f64 = digits.parse().map_err(|_| format!("invalid size '{text}'"))?;
    let mult = match unit {
        "" => default_unit,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        "t" => 1 << 40,
        _ => return Err(format!("invalid size '{text}'")),
    };
    Ok((n * mult as f64) as u64)
}

fn fmt_size(n: u64) -> String {
    let mut v = n as f64;
    for unit in ["B", "KB", "MB", "GB", "TB"] {
        if v < 1024.0 || unit == "TB" {
            return if unit == "B" { format!("{v:.0} {unit}") } else { format!("{v:.1} {unit}") };
        }
        v /= 1024.0;
    }
    unreachable!()
}

struct Bar {
    enabled: bool,
    started: Instant,
    last: Mutex<Instant>,
}

impl Bar {
    fn new(enabled: bool) -> Self {
        Bar { enabled: enabled && std::io::stderr().is_terminal(), started: Instant::now(), last: Mutex::new(Instant::now()) }
    }

    fn clear(&self) {
        if self.enabled {
            eprint!("\r{:79}\r", "");
        }
    }
}

impl Hooks for Bar {
    fn progress(&self, done: u64, total: u64) -> bool {
        if !self.enabled {
            return true;
        }
        let mut last = self.last.lock().unwrap();
        if last.elapsed().as_millis() < 100 && done < total {
            return true;
        }
        *last = Instant::now();
        let secs = self.started.elapsed().as_secs_f64().max(1e-9);
        let frac = if total > 0 { (done as f64 / total as f64).min(1.0) } else { 0.0 };
        let filled = (frac * 30.0) as usize;
        eprint!(
            "\r[{}{}] {:5.1}%  {} / {}  {}/s ",
            "#".repeat(filled),
            "-".repeat(30 - filled),
            frac * 100.0,
            fmt_size(done),
            fmt_size(total),
            fmt_size((done as f64 / secs) as u64)
        );
        let _ = std::io::stderr().flush();
        true
    }
}

fn options(a: &Args) -> Result<ConvertOptions, String> {
    let mut o = ConvertOptions { overwrite: a.has("overwrite") || a.has("f"), ..Default::default() };
    o.to = a.get("to").map(str::to_string);
    o.from = a.get("from").map(str::to_string);
    o.profile = a.get("profile").map(str::to_string);
    o.compression = a.get("compression").map(str::to_string);
    o.level = a.num("level")?;
    o.types = match a.get("types") {
        None | Some("infer") => TypesMode::Infer,
        Some("string") => TypesMode::String,
        Some("loose") => TypesMode::Loose,
        Some(other) => return Err(format!("--types must be infer, string or loose, not '{other}'")),
    };
    if let Some(spec) = a.get("schema") {
        for part in spec.split(',').filter(|p| !p.trim().is_empty()) {
            let (k, v) = part.split_once('=').ok_or_else(|| format!("invalid --schema entry '{part}'"))?;
            o.schema.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    o.sniff.delimiter = a.get("delimiter").map(str::to_string);
    o.sniff.encoding = a.get("encoding").map(str::to_string);
    o.sniff.quote = a.get("quote").map(str::to_string);
    if a.has("no-header") {
        o.sniff.header = Some(false);
    }
    o.sniff.skip_rows = a.num("skip-rows")?;
    o.bad_lines = match a.get("bad-lines") {
        None | Some("quarantine") => BadLines::Quarantine,
        Some("skip") => BadLines::Skip,
        Some("error") => BadLines::Error,
        Some("pad") => BadLines::Pad,
        Some(other) => return Err(format!("--bad-lines must be quarantine, skip, error or pad, not '{other}'")),
    };
    o.quarantine = a.get("quarantine").map(str::to_string);
    o.columns = a.get("columns").map(|c| c.split(',').map(|s| s.trim().to_string()).collect());
    o.limit = a.num("limit")?;
    o.infer = if a.get("infer") == Some("full") { InferMode::Full } else { InferMode::Sample };
    o.threads = a.num("threads")?;
    o.memory = a.get("memory").map(|m| parse_size(m, 1 << 20)).transpose()?;
    o.out_delimiter = a.get("out-delimiter").map(str::to_string);
    o.out_crlf = a.has("crlf");
    o.null_values = a.get("null-values").map(|v| v.split(',').map(str::to_string).collect()).unwrap_or_default();
    Ok(o)
}

fn print_report(r: &Report, a: &Args) {
    if a.has("json") {
        println!("{}", serde_json::to_string_pretty(r).unwrap_or_default());
        return;
    }
    if a.has("quiet") || a.has("q") {
        return;
    }
    let ratio = if r.bytes_out > 0 && r.bytes_out <= r.bytes_in {
        format!(" ({:.1}x smaller)", r.bytes_in as f64 / r.bytes_out as f64)
    } else {
        String::new()
    };
    let rows = if r.rows > 0 { format!("{} rows, ", r.rows) } else { String::new() };
    println!(
        "{} -> {}: {}{} -> {}{} in {:.2} s [{} threads]",
        r.source,
        r.destination,
        rows,
        fmt_size(r.bytes_in),
        fmt_size(r.bytes_out),
        ratio,
        r.seconds,
        r.threads
    );
    for w in &r.warnings {
        eprintln!("  note: {w}");
    }
    for p in &r.parts {
        println!("  {}: {} rows, {}", p.path, p.rows, fmt_size(p.bytes));
    }
}

fn need<'a>(a: &'a Args, i: usize, what: &str) -> Result<&'a str, String> {
    a.positional.get(i).map(String::as_str).ok_or_else(|| format!("missing {what}\n\n{USAGE}"))
}

fn run(raw: Vec<String>) -> Result<(), String> {
    let a = parse_args(&raw);
    let cmd = a.positional.first().map(String::as_str).unwrap_or("");
    if cmd.is_empty() || a.has("help") || a.has("h") {
        print!("{USAGE}");
        return Ok(());
    }
    let quiet = a.has("quiet") || a.has("q") || a.has("json");
    let bar = Bar::new(!quiet);
    let err = |e: Error| e.to_string();
    match cmd {
        "convert" => {
            let src = need(&a, 1, "source")?;
            let dst = need(&a, 2, "destination")?;
            let r = convert::convert(Path::new(src), Path::new(dst), &options(&a)?, &bar).map_err(err)?;
            bar.clear();
            print_report(&r, &a);
        }
        "compress" => {
            let src = PathBuf::from(need(&a, 1, "source")?);
            let dst = a.positional.get(2).map(PathBuf::from).unwrap_or_else(|| {
                let mut s = src.clone().into_os_string();
                s.push(".dcz");
                PathBuf::from(s)
            });
            let profile = Profile::parse(a.get("profile").unwrap_or("balanced")).map_err(err)?;
            let opts = dcz::EncodeOptions { profile, threads: a.num("threads")?.unwrap_or(0), block_size: None };
            let r = dcz::compress_file(&src, &dst, &opts, a.has("overwrite") || a.has("f"), &bar).map_err(err)?;
            bar.clear();
            print_report(&r, &a);
        }
        "decompress" => {
            let src = PathBuf::from(need(&a, 1, "source")?);
            let dst = a.positional.get(2).map(PathBuf::from).unwrap_or_else(|| {
                if src.extension().map_or(false, |e| e.eq_ignore_ascii_case("dcz")) {
                    src.with_extension("")
                } else {
                    src.with_extension("out")
                }
            });
            let r = dcz::decompress_file(&src, &dst, a.has("overwrite") || a.has("f"), a.num("threads")?.unwrap_or(0), &bar)
                .map_err(err)?;
            bar.clear();
            print_report(&r, &a);
        }
        "split" => {
            let src = need(&a, 1, "source")?;
            let dir = need(&a, 2, "output directory")?;
            let mut o = options(&a)?;
            o.split_rows = a.num("rows")?;
            o.split_bytes = a.get("size").map(|s| parse_size(s, 1 << 20)).transpose()?;
            if o.split_rows.is_none() && o.split_bytes.is_none() {
                return Err("split needs --rows N or --size SIZE".into());
            }
            o.split_pattern = a.get("pattern").map(str::to_string);
            let r = convert::convert(Path::new(src), Path::new(dir), &o, &bar).map_err(err)?;
            bar.clear();
            print_report(&r, &a);
        }
        "info" => {
            let v = inspect::info(Path::new(need(&a, 1, "file")?), a.has("exact"), &options(&a)?).map_err(err)?;
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
        }
        "head" => {
            let n: usize = a.num("n")?.unwrap_or(10);
            let p = inspect::head(Path::new(need(&a, 1, "file")?), n, &options(&a)?).map_err(err)?;
            if a.has("json") {
                println!("{}", serde_json::to_string_pretty(&p).unwrap_or_default());
            } else {
                println!("{}", p.columns.join("\t"));
                for row in &p.rows {
                    println!("{}", row.iter().map(|v| v.clone().unwrap_or_default()).collect::<Vec<_>>().join("\t"));
                }
            }
        }
        "count" => println!("{}", inspect::count(Path::new(need(&a, 1, "file")?), &options(&a)?).map_err(err)?),
        "sniff" => {
            let d = convert::sniff_file(Path::new(need(&a, 1, "file")?), &options(&a)?.sniff).map_err(err)?;
            println!("{}", serde_json::to_string_pretty(&d).unwrap_or_default());
        }
        "doctor" => {
            let info = resources::detect();
            let plan = resources::plan(Some(10 << 30), None, None, None);
            println!("dataconv native {}", dataconv_core::VERSION);
            println!("system  {} {} ({}-bit)", info.os, info.arch, info.pointer_width);
            println!("cpu     {} usable of {} logical", info.cpu_usable, info.cpu_logical);
            println!(
                "memory  {} available of {}{}",
                fmt_size(info.mem_available),
                fmt_size(info.mem_total),
                info.cgroup_limit.map(|l| format!(", container limit {}", fmt_size(l))).unwrap_or_default()
            );
            println!(
                "plan    a 10 GB job: {} threads, {} budget, {} chunks",
                plan.threads,
                fmt_size(plan.memory_budget),
                fmt_size(plan.chunk_size as u64)
            );
        }
        "version" | "--version" => println!("dataconv native {}", dataconv_core::VERSION),
        other => return Err(format!("unknown command '{other}'\n\n{USAGE}")),
    }
    Ok(())
}

fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.first().map(String::as_str) == Some("--version") {
        println!("dataconv native {}", dataconv_core::VERSION);
        return ExitCode::SUCCESS;
    }
    match run(raw) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("dataconv: error: {e}");
            ExitCode::FAILURE
        }
    }
}
