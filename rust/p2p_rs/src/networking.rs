use std::pin::Pin;
use std::sync::Arc;

use crate::ext::{PyErrExt as _, ResultExt as _};
use futures_lite::{Stream, StreamExt as _};
use networking::swarm::{FromSwarm, Swarm, ToSwarm, create_swarm};
use networking::{Session, is_valid_zid};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{Bound, PyAny, PyErr, PyResult, Python, pymethods};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use tokio::sync::{Mutex, mpsc, oneshot};

#[gen_stub_pyclass]
#[pyclass(name = "NetworkingHandle")]
pub struct PyNetworkingHandle {
    // channels
    pub to_swarm: mpsc::Sender<ToSwarm>,
    pub swarm: Arc<Mutex<Pin<Box<dyn Stream<Item = FromSwarm> + Send>>>>,
}

#[gen_stub_pyclass]
#[pyclass(name = "FromSwarm")]
#[derive(Clone)]
pub struct PyFromSwarm {
    #[pyo3(get)]
    pub is_connection: bool,
    #[pyo3(get)]
    pub connected: bool,
    #[pyo3(get)]
    pub peer_id: Option<String>,
    #[pyo3(get)]
    pub topic: Option<String>,
    pub data: Vec<u8>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFromSwarm {
    #[getter]
    pub fn is_message(&self) -> bool {
        !self.is_connection
    }

    #[getter]
    pub fn data<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        if self.is_connection {
            None
        } else {
            Some(PyBytes::new(py, &self.data))
        }
    }

    fn __repr__(&self) -> String {
        if self.is_connection {
            format!(
                "FromSwarm.Connection(connected={}, peer_id='{}')",
                self.connected,
                self.peer_id.as_deref().unwrap_or("")
            )
        } else {
            format!(
                "FromSwarm.Message(topic='{}', bytes={})",
                self.topic.as_deref().unwrap_or(""),
                self.data.len()
            )
        }
    }
}

impl From<FromSwarm> for PyFromSwarm {
    fn from(value: FromSwarm) -> Self {
        match value {
            FromSwarm::Discovered { peer_id } => Self {
                is_connection: true,
                connected: true,
                peer_id: Some(peer_id),
                topic: None,
                data: Vec::new(),
            },
            FromSwarm::Expired { peer_id } => Self {
                is_connection: true,
                connected: false,
                peer_id: Some(peer_id),
                topic: None,
                data: Vec::new(),
            },
            FromSwarm::Message { topic, data } => Self {
                is_connection: false,
                connected: false,
                peer_id: None,
                topic: Some(topic),
                data,
            },
        }
    }
}

impl PyNetworkingHandle {
    pub fn from_session(session: Session) -> Self {
        let (to_swarm, from_client) = mpsc::channel(1024);
        let swarm = Swarm {
            from_client,
            session,
        };
        PyNetworkingHandle {
            swarm: Arc::new(Mutex::new(swarm.into_stream())),
            to_swarm,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyNetworkingHandle {
    // NOTE: `async fn`s here that use `.await` will wrap the future in `.allow_threads_py()`
    //       immediately beforehand to release the interpreter.
    //       SEE: https://pyo3.rs/v0.26.0/async-await.html#detaching-from-the-interpreter-across-await

    // ---- Lifecycle management methods ----

    #[staticmethod]
    pub fn new(
        identity: &str,
        namespace: &str,
        listen_port: u16,
        discovery_service_port: u16,
    ) -> PyResult<PyNetworkingHandle> {
        // todo: zenoh self assigned peers
        if listen_port == 0 {
            todo!("cannot listen on port 0 yet");
        }
        // create communication channels
        let (to_swarm, from_client) = mpsc::channel(1024);

        // get identity
        if !is_valid_zid(identity) {
            return Err(PyValueError::new_err(format!(
                "{identity} is not a valid zenoh identity"
            )));
        }

        // create networking swarm (within tokio context!! or it crashes)
        let swarm = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(create_swarm(
                identity,
                namespace,
                from_client,
                listen_port,
                discovery_service_port,
            ))
            .pyerr()?;

        Ok(PyNetworkingHandle {
            swarm: Arc::new(Mutex::new(swarm.into_stream())),
            to_swarm,
        })
    }

    #[gen_stub(override_return_type(
        type_repr="typing.Awaitable[FromSwarm]", imports=("typing")
    ))]
    pub fn recv<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let swarm = Arc::clone(&self.swarm);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = swarm.lock().await;
            guard
                .next()
                .await
                .ok_or_else(PyErr::receiver_channel_closed)
                .map(PyFromSwarm::from)
        })
    }

    // ---- Gossipsub management methods ----

    /// Subscribe to a `GossipSub` topic.
    pub fn gossipsub_subscribe<'py>(
        &'py self,
        py: Python<'py>,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let to_swarm = self.to_swarm.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (tx, rx) = oneshot::channel();
            to_swarm
                .send(ToSwarm::Subscribe {
                    topic,
                    result_sender: tx,
                })
                .await
                .map_err(|_| PyErr::receiver_channel_closed())?;

            rx.await
                .map_err(|_| PyErr::receiver_channel_closed())?
                .pyerr()
        })
    }

    /// Unsubscribes from a `GossipSub` topic.
    pub fn gossipsub_unsubscribe<'py>(
        &'py self,
        py: Python<'py>,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let to_swarm = self.to_swarm.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (tx, rx) = oneshot::channel();
            to_swarm
                .send(ToSwarm::Unsubscribe {
                    topic,
                    result_sender: tx,
                })
                .await
                .map_err(|_| PyErr::receiver_channel_closed())?;

            rx.await
                .map_err(|_| PyErr::receiver_channel_closed())
        })
    }

    /// Publishes a message to the `GossipSub` network.
    pub fn gossipsub_publish<'py>(
        &'py self,
        py: Python<'py>,
        topic: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let to_swarm = self.to_swarm.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (tx, rx) = oneshot::channel();
            to_swarm
                .send(ToSwarm::Publish {
                    topic,
                    data,
                    result_sender: tx,
                })
                .await
                .map_err(|_| PyErr::receiver_channel_closed())?;

            rx.await
                .map_err(|_| PyErr::receiver_channel_closed())?
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

pub fn networking_submodule(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNetworkingHandle>()?;
    m.add_class::<PyFromSwarm>()?;

    Ok(())
}
