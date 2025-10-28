use pyo3::prelude::*;
use dbt_sa_lib::dbt_sa_lib::run_with_args;
use dbt_sa_lib::dbt_sa_clap::Cli;


/// Invoke dbt
#[pyfunction]
fn invoke(args: &str) -> PyResult<String>
{
    Ok(args.to_owned())
}

/// A Python module implemented in Rust.
#[pymodule]
fn dbt_fusion(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(invoke, m)?)?;
    Ok(())
}
