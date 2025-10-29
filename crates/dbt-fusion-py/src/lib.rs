use clap::Parser;
use clap::error::ErrorKind;
use pyo3::prelude::*;
use pyo3::exceptions::{PySystemExit, PyTypeError, PyValueError};
use dbt_sa_lib::dbt_sa_lib::{run_with_args, print_trimmed_error};
use dbt_sa_lib::dbt_sa_clap::Cli;
use pyo3::types::{PyList, PyString};
use dbt_common::cancellation::CancellationTokenSource;


// JAKE: `import dbt_fusion; dbt_fusion.invoke(["dbt", "deps", "--project-dir", "/Users/j293015/Repos/insightsos-dbt"])`

/// Invoke dbt
#[pyfunction]
fn invoke(args: &Bound<'_, PyList>) -> PyResult<()>
{
    let mut str_args: Vec<String> = Vec::new();

    // We are responsible for adding the dbt command at the beginning for the parser
    str_args.push("dbt".to_owned());

    // Validate all items pass into list are strings
    for (idx, item) in args.iter().enumerate() {
        if !item.is_instance_of::<PyString>() {
            let err_str = format!("invoke() must recieve a list of strings. The object at position {} in the list is not a string.", idx);
            return Err(PyTypeError::new_err(err_str));
        }

        let s = item.extract::<String>()?;

        // Prevent users from adding the dbt command itself, we handle that
        if idx == 0 && s.trim() == "dbt" {
            let err_str = "invoke() should only recieve subcommands and arguments for the dbt CLI command, don't pass in `dbt` at the beginning - e.g. [\"init\"], not [\"dbt\", \"init\"]";
            return Err(PyValueError::new_err(err_str));
        }

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

    let status_code = run_with_args(cli, token);

    if status_code != 0 {
        // JAKE: is PySystemExit the right exception type?
        return Err(PySystemExit::new_err(format!("dbt exited with code {}.", status_code)));
    }

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn dbt_fusion(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(invoke, m)?)?;
    Ok(())
}
