//! p2p_rs: PyO3 native module for P2P networking

mod networking;

use crate::networking::networking_submodule;
use pyo3::prelude::PyModule;
use pyo3::{Bound, PyResult, pymodule};
use pyo3_stub_gen::define_stub_info_gatherer;

/// Namespace for crate-wide extension traits/methods
pub(crate) mod ext {
    use extend::ext;
    use pyo3::exceptions::{PyConnectionError, PyRuntimeError};
    use pyo3::{PyErr, PyResult};

    #[ext(pub, name = ResultExt)]
    impl<T, E> Result<T, E>
    where
        E: ToString,
    {
        fn pyerr(self) -> PyResult<T> {
            self.map_err(|e| PyRuntimeError::new_err(e.to_string()))
        }
    }

    #[ext(pub, name = PyErrExt)]
    impl PyErr {
        fn receiver_channel_closed() -> Self {
            PyConnectionError::new_err("Receiver channel closed unexpectedly")
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule(name = "p2p_rs", gil_used = true)]
fn main_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = env_logger::try_init();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_async_runtimes::tokio::init(builder);

    networking_submodule(m)?;

    Ok(())
}

define_stub_info_gatherer!(stub_info);
