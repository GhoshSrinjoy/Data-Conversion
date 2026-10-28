//! Python bindings for vegg-core: the `vegg._rust` extension module of the vegg package.
//!
//! Options and reports cross the boundary as JSON strings, which keeps the
//! interface stable across versions.  Long-running calls release the GIL; progress
//! callbacks re-acquire it briefly and also check for Ctrl-C.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use vegg_core::codec::Profile;
use vegg_core::convert::{self as engine, ConvertOptions, Hooks};
use vegg_core::pack::{self, Packed, Values};
use vegg_core::{dcz, inspect, resources, Error};

/// Bumped whenever the JSON contract with the Python package changes.
const PROTOCOL: u32 = 1;

create_exception!(vegg._rust, NativeError, PyException, "Error raised by the native engine: (kind, message).");

fn to_py(err: Error) -> PyErr {
    if matches!(err, Error::Cancelled) {
        return pyo3::exceptions::PyKeyboardInterrupt::new_err("cancelled");
    }
    NativeError::new_err((err.kind().to_string(), err.to_string()))
}

struct PyHooks {
    callback: Option<Py<PyAny>>,
    last: Mutex<Instant>,
    error: Mutex<Option<PyErr>>,
    stop: AtomicBool,
}

impl PyHooks {
    fn new(callback: Option<Py<PyAny>>) -> Self {
        PyHooks {
            callback,
            last: Mutex::new(Instant::now() - Duration::from_secs(1)),
            error: Mutex::new(None),
            stop: AtomicBool::new(false),
        }
    }

    fn finish<T>(self, result: Result<T, Error>) -> PyResult<T> {
        if let Some(e) = self.error.lock().unwrap().take() {
            return Err(e);
        }
        result.map_err(to_py)
    }
}

impl Hooks for PyHooks {
    fn progress(&self, done: u64, total: u64) -> bool {
        if self.stop.load(Ordering::Relaxed) {
            return false;
        }
        {
            let mut last = self.last.lock().unwrap();
            if last.elapsed() < Duration::from_millis(100) && done < total {
                return true;
            }
            *last = Instant::now();
        }
        Python::attach(|py| {
            if let Err(e) = py.check_signals() {
                *self.error.lock().unwrap() = Some(e);
                self.stop.store(true, Ordering::Relaxed);
                return false;
            }
            if let Some(cb) = &self.callback {
                match cb.bind(py).call1((done, total)) {
                    Ok(r) => {
                        if r.is_none() {
                            return true;
                        }
                        let keep = r.is_truthy().unwrap_or(true);
                        if !keep {
                            self.stop.store(true, Ordering::Relaxed);
                        }
                        keep
                    }
                    Err(e) => {
                        *self.error.lock().unwrap() = Some(e);
                        self.stop.store(true, Ordering::Relaxed);
                        false
                    }
                }
            } else {
                true
            }
        })
    }
}

fn parse_options(json: &str) -> PyResult<ConvertOptions> {
    if json.trim().is_empty() {
        return Ok(ConvertOptions::default());
    }
    serde_json::from_str(json).map_err(|e| NativeError::new_err(("invalid".to_string(), format!("bad options: {e}"))))
}

fn json<T: serde::Serialize>(value: &T) -> PyResult<String> {
    serde_json::to_string(value).map_err(|e| NativeError::new_err(("invalid".to_string(), e.to_string())))
}

#[pyfunction]
fn version() -> &'static str {
    vegg_core::VERSION
}

#[pyfunction]
fn protocol() -> u32 {
    PROTOCOL
}

#[pyfunction]
fn system_info() -> PyResult<String> {
    json(&resources::detect())
}

#[pyfunction]
#[pyo3(signature = (input_size=None, threads=None, memory=None, chunk_size=None))]
fn plan(
    input_size: Option<u64>,
    threads: Option<usize>,
    memory: Option<u64>,
    chunk_size: Option<usize>,
) -> PyResult<String> {
    json(&resources::plan(input_size, threads, memory, chunk_size))
}

#[pyfunction]
#[pyo3(signature = (path, options="{}"))]
fn sniff(py: Python<'_>, path: &str, options: &str) -> PyResult<String> {
    let opts = parse_options(options)?;
    let d = py.detach(|| engine::sniff_file(Path::new(path), &opts.sniff)).map_err(to_py)?;
    json(&d)
}

#[pyfunction]
#[pyo3(signature = (src, dst, options="{}", progress=None))]
fn convert(py: Python<'_>, src: &str, dst: &str, options: &str, progress: Option<Py<PyAny>>) -> PyResult<String> {
    let opts = parse_options(options)?;
    let hooks = PyHooks::new(progress);
    let result = py.detach(|| engine::convert(Path::new(src), Path::new(dst), &opts, &hooks));
    json(&hooks.finish(result)?)
}

#[pyfunction]
#[pyo3(signature = (src, dst, profile="balanced", threads=0, block_size=None, overwrite=false, progress=None, memory=None))]
#[allow(clippy::too_many_arguments)]
fn compress(
    py: Python<'_>,
    src: &str,
    dst: &str,
    profile: &str,
    threads: usize,
    block_size: Option<usize>,
    overwrite: bool,
    progress: Option<Py<PyAny>>,
    memory: Option<u64>,
) -> PyResult<String> {
    let profile = Profile::parse(profile).map_err(to_py)?;
    let opts = dcz::EncodeOptions { profile, threads, block_size, memory };
    let hooks = PyHooks::new(progress);
    let result = py.detach(|| dcz::compress_file(Path::new(src), Path::new(dst), &opts, overwrite, &hooks));
    json(&hooks.finish(result)?)
}

#[pyfunction]
#[pyo3(signature = (src, dst, threads=0, overwrite=false, progress=None, memory=None))]
fn decompress(
    py: Python<'_>,
    src: &str,
    dst: &str,
    threads: usize,
    overwrite: bool,
    progress: Option<Py<PyAny>>,
    memory: Option<u64>,
) -> PyResult<String> {
    let hooks = PyHooks::new(progress);
    let result = py.detach(|| dcz::decompress_file(Path::new(src), Path::new(dst), overwrite, threads, memory, &hooks));
    json(&hooks.finish(result)?)
}

#[pyfunction]
fn dcz_footer(py: Python<'_>, path: &str) -> PyResult<String> {
    let f = py.detach(|| dcz::read_footer(Path::new(path))).map_err(to_py)?;
    json(&f)
}

#[pyfunction]
#[pyo3(signature = (data, csv=true, profile="balanced"))]
fn compress_bytes<'py>(py: Python<'py>, data: &[u8], csv: bool, profile: &str) -> PyResult<Bound<'py, PyBytes>> {
    let profile = Profile::parse(profile).map_err(to_py)?;
    let owned = data.to_vec();
    let out = py.detach(move || dcz::compress_bytes(&owned, csv, profile)).map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

#[pyfunction]
fn decompress_bytes<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let owned = data.to_vec();
    let out = py.detach(move || dcz::decompress_bytes(&owned)).map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

#[pyfunction]
#[pyo3(signature = (path, exact=false, options="{}"))]
fn info(py: Python<'_>, path: &str, exact: bool, options: &str) -> PyResult<String> {
    let opts = parse_options(options)?;
    let v = py.detach(|| inspect::info(Path::new(path), exact, &opts)).map_err(to_py)?;
    json(&v)
}

#[pyfunction]
#[pyo3(signature = (path, n=10, options="{}"))]
fn head(py: Python<'_>, path: &str, n: usize, options: &str) -> PyResult<String> {
    let opts = parse_options(options)?;
    let p = py.detach(|| inspect::head(Path::new(path), n, &opts)).map_err(to_py)?;
    json(&p)
}

#[pyfunction]
#[pyo3(signature = (path, options="{}"))]
fn count(py: Python<'_>, path: &str, options: &str) -> PyResult<u64> {
    let opts = parse_options(options)?;
    py.detach(|| inspect::count(Path::new(path), &opts)).map_err(to_py)
}

fn le_chunks<const N: usize>(data: &[u8]) -> PyResult<impl Iterator<Item = [u8; N]> + '_> {
    if !data.len().is_multiple_of(N) {
        return Err(NativeError::new_err(("invalid".to_string(), format!("buffer length must be a multiple of {N}"))));
    }
    Ok(data.as_chunks::<N>().0.iter().copied())
}

fn bitmap_from(valid: Option<&[u8]>) -> Option<Vec<bool>> {
    valid.map(|v| v.iter().map(|&b| b != 0).collect())
}

/// Pack values.  `kind` is "int" / "float" (little-endian 8-byte buffers), "bool"
/// (one byte per value) or "str" (`values` is a list of str).  `valid` holds one byte
/// per position (0 = missing) and `values` only the present ones.
#[pyfunction]
#[pyo3(signature = (kind, values, valid=None, tag=b"".as_slice(), profile="balanced"))]
fn pack_values<'py>(
    py: Python<'py>,
    kind: &str,
    values: &Bound<'py, PyAny>,
    valid: Option<&[u8]>,
    tag: &[u8],
    profile: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let profile = Profile::parse(profile).map_err(to_py)?;
    let vals = match kind {
        "int" => Values::Int(le_chunks::<8>(values.extract::<&[u8]>()?)?.map(i64::from_le_bytes).collect()),
        "float" => Values::Float(le_chunks::<8>(values.extract::<&[u8]>()?)?.map(f64::from_le_bytes).collect()),
        "bool" => Values::Bool(values.extract::<&[u8]>()?.iter().map(|&b| b != 0).collect()),
        "str" => Values::Str(values.extract::<Vec<String>>()?.into_iter().map(String::into_bytes).collect()),
        other => return Err(NativeError::new_err(("invalid".to_string(), format!("unknown value kind '{other}'")))),
    };
    let packed = Packed { values: vals, valid: bitmap_from(valid), tag: tag.to_vec() };
    let out = py.detach(move || pack::pack(&packed, profile)).map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// (kind, values, validity, tag) as returned to Python.
type Unpacked<'py> = (String, Bound<'py, PyAny>, Option<Bound<'py, PyBytes>>, Bound<'py, PyBytes>);

/// Inverse of `pack_values`: returns (kind, values, valid, tag) with the same
/// buffer conventions.
#[pyfunction]
fn unpack_values<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Unpacked<'py>> {
    let owned = data.to_vec();
    let p = py.detach(move || pack::unpack(&owned)).map_err(to_py)?;
    let valid = p.valid.as_ref().map(|v| PyBytes::new(py, &v.iter().map(|&b| b as u8).collect::<Vec<u8>>()));
    let tag = PyBytes::new(py, &p.tag);
    let (kind, values): (&str, Bound<'py, PyAny>) = match p.values {
        Values::Int(v) => {
            ("int", PyBytes::new(py, &v.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>()).into_any())
        }
        Values::Float(v) => {
            ("float", PyBytes::new(py, &v.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>()).into_any())
        }
        Values::Bool(v) => ("bool", PyBytes::new(py, &v.iter().map(|&b| b as u8).collect::<Vec<u8>>()).into_any()),
        Values::Str(v) => {
            let strings: Vec<String> = v
                .into_iter()
                .map(|b| {
                    String::from_utf8(b).map_err(|_| {
                        NativeError::new_err(("integrity".to_string(), "invalid UTF-8 in packed strings".to_string()))
                    })
                })
                .collect::<PyResult<_>>()?;
            ("str", PyList::new(py, strings)?.into_any())
        }
    };
    Ok((kind.to_string(), values, valid, tag))
}

#[pymodule]
#[pyo3(name = "_rust")]
fn rust_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("NativeError", m.py().get_type::<NativeError>())?;
    m.add("__version__", vegg_core::VERSION)?;
    for f in [
        wrap_pyfunction!(version, m)?,
        wrap_pyfunction!(protocol, m)?,
        wrap_pyfunction!(system_info, m)?,
        wrap_pyfunction!(plan, m)?,
        wrap_pyfunction!(sniff, m)?,
        wrap_pyfunction!(convert, m)?,
        wrap_pyfunction!(compress, m)?,
        wrap_pyfunction!(decompress, m)?,
        wrap_pyfunction!(dcz_footer, m)?,
        wrap_pyfunction!(compress_bytes, m)?,
        wrap_pyfunction!(decompress_bytes, m)?,
        wrap_pyfunction!(info, m)?,
        wrap_pyfunction!(head, m)?,
        wrap_pyfunction!(count, m)?,
        wrap_pyfunction!(pack_values, m)?,
        wrap_pyfunction!(unpack_values, m)?,
    ] {
        m.add_function(f)?;
    }
    Ok(())
}
