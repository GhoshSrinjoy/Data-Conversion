use pyo3::prelude::*;

#[pyfunction]
fn hello() -> &'static str { dataconv_core::hello() }

#[pymodule]
fn dataconv_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    Ok(())
}
