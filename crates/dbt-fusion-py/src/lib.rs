use clap::Parser;
use clap::error::ErrorKind;
use pyo3::prelude::*;
use pyo3::exceptions::PyTypeError;
use dbt_sa_lib::dbt_sa_lib::{run_with_args, print_trimmed_error};
use dbt_sa_lib::dbt_sa_clap::Cli;
use pyo3::types::{PyList, PyString};
use dbt_common::cancellation::CancellationTokenSource;
use std::process::ExitCode;

/// Invoke dbt
#[pyfunction]
fn invoke(args: &Bound<'_, PyList>) -> PyResult<String>
{
    let mut str_args: Vec<String> = Vec::new();

    // Validate all items pass into list are strings
    for (idx, item) in args.iter().enumerate() {
        if !item.is_instance_of::<PyString>() {
            let err_str = format!("invoke() must recieve a list of strings. The object at position {} in the list is not a string.", idx);
            return Err(PyTypeError::new_err(err_str));
        }

        let s = item.extract::<String>()?;
        str_args.push(s);
    }
    
    // Cancellation token stuff??
    let cst = CancellationTokenSource::new();
    let token = cst.token();

    let cli = match Cli::try_parse_from(str_args.iter()) {
        Ok(cli) => cli,
        Err(e) => {
            if e.kind() == ErrorKind::UnknownArgument {
                // todo make this for more than just unknown arguments
                // Only show the actual error message
                let msg = e.to_string(); // includes both "error:" and possibly "tip:"
                print_trimmed_error(msg); // prints to stderr
                std::process::exit(1);
            } else {
                // For other errors, show full help as usual
                e.exit();
            }
        }
    };

    let exit_code = run_with_args(cli, token);

    Ok("Success!".to_owned())
}

/// A Python module implemented in Rust.
#[pymodule]
fn dbt_fusion(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(invoke, m)?)?;
    Ok(())
}
