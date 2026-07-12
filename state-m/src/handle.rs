use crate::{
    reader::Reader,
    source::{AsSourceState, Source},
    state::{State, StateEvent},
};
use chrono::Utc;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio::{
    select,
    sync::{
        Mutex,
        broadcast::{
            Receiver as Recver,
            error::{RecvError, SendError},
        },
    },
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

#[derive(Clone, Debug)]
pub(crate) struct Handle<S>
where
    S: 'static + AsSourceState,
{
    inner: HandleI<S>,
    cache: Arc<RwLock<State<S>>>,
    cancel_token: CancellationToken,
}

#[derive(Clone, Debug)]
enum HandleI<S>
where
    S: 'static + AsSourceState,
{
    Source(Source<S>, Arc<RwLock<State<S>>>),
    Reader(Reader<S>),
}

impl<S> Handle<S>
where
    S: 'static + AsSourceState,
{
    fn recver(&self) -> Arc<Mutex<Recver<StateEvent<S>>>> {
        match self.inner {
            HandleI::Source(ref source, _) => source.recver.clone(),
            HandleI::Reader(ref reader) => reader.recver.clone(),
        }
    }

    #[instrument(level = "trace", skip(self, f))]
    async fn inner_change(
        &self,
        f: impl FnOnce(S) -> S,
        is_touch: bool,
        wait_arrival: bool,
    ) -> Result<(), StateChangeError<S>> {
        match self.inner {
            HandleI::Source(ref source, ref cache) => {
                let mut guard = cache.write().await;
                let s_old = (*guard).value.clone();
                let s = f(s_old.clone());
                if is_touch || s_old != s {
                    let (event, wait_rx) = {
                        let state = State {
                            value: s,
                            timestamp: Utc::now(),
                        };
                        if wait_arrival {
                            let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel(1);
                            let event = StateEvent {
                                state,
                                is_touch,
                                close_handle: Some(tx),
                            };
                            (event, Some(rx))
                        } else {
                            let event = StateEvent {
                                state,
                                is_touch,
                                close_handle: None,
                            };
                            (event, None)
                        }
                    };
                    let state = event.state.clone();
                    let recver_count = source.sender.send(event)?;
                    *guard = state.clone();
                    tracing::debug!("{recver_count} | send -- {state:?}");
                    if let Some(mut rx) = wait_rx {
                        _ = rx.recv().await;
                        tracing::debug!("done -- {state:?}");
                    }
                }
            }
            HandleI::Reader(_) => return Err(StateChangeError::StateReadOnly),
        }
        Ok(())
    }
}

impl<S> Handle<S>
where
    S: 'static + AsSourceState,
{
    pub fn from_source(source: Source<S>) -> Self {
        Self {
            inner: HandleI::Source(source, Default::default()),
            cache: Default::default(),
            cancel_token: Default::default(),
        }
    }

    pub fn from_reader(reader: Reader<S>) -> Self {
        Self {
            inner: HandleI::Reader(reader),
            cache: Default::default(),
            cancel_token: Default::default(),
        }
    }

    pub fn reader(&self) -> Reader<S> {
        match self.inner {
            HandleI::Source(ref source, _) => source.reader(),
            HandleI::Reader(ref reader) => reader.clone(),
        }
    }

    pub async fn recv(&self) -> Result<Option<(State<S>, State<S>)>, RecvError> {
        let res = {
            let recver = self.recver();
            let mut guard = recver.lock().await;
            select! {
                _ = self.cancel_token.cancelled() => Err(RecvError::Closed),
                res = (*guard).recv() => res
            }
        };
        match res {
            Ok(r) => {
                let s_old = { self.cache.read().await.clone() };
                if r.is_touch || r.state.value != s_old.value {
                    tracing::debug!("recv -- {:?}", r.state);
                    {
                        *self.cache.write().await = r.state.clone();
                    }
                    Ok(Some((r.state, s_old)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    pub async fn value(&self) -> S {
        self.cache.read().await.value.clone()
    }

    pub async fn state(&self) -> State<S> {
        self.cache.read().await.clone()
    }

    pub async fn touch(&self) -> Result<(), StateChangeError<S>> {
        self.inner_change(|s| s.clone(), true, false).await
    }

    pub async fn wait_touch(&self) -> Result<(), StateChangeError<S>> {
        self.inner_change(|s| s.clone(), true, true).await
    }

    pub async fn alter(&self, s: S) -> Result<(), StateChangeError<S>> {
        self.inner_change(|_| s, false, false).await
    }

    pub async fn wait_alter(&self, s: S) -> Result<(), StateChangeError<S>> {
        self.inner_change(|_| s, false, true).await
    }

    pub async fn amend(&self, f: impl FnOnce(S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, false, false).await
    }

    pub async fn wait_amend(&self, f: impl FnOnce(S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, false, false).await
    }
}

#[derive(Debug, Error)]
pub enum StateChangeError<S>
where
    S: Default,
{
    #[error("This state is read only.")]
    StateReadOnly,
    #[error(transparent)]
    SendError(#[from] SendError<StateEvent<S>>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
