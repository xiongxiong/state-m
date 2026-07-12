use crate::{
    reader::Reader,
    source::{AsSourceState, Source},
    state::{State, StateEvent},
};
use chrono::Utc;
use crossfire::{
    MAsyncRx, MAsyncTx, RecvError, SendError,
    mpmc::{self, List},
    null::CloseHandle,
};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::instrument;

#[derive(Clone, Debug)]
pub(crate) enum Handle<S>
where
    S: 'static + AsSourceState,
{
    Source(Source<S>, Arc<RwLock<State<S>>>),
    Reader(Reader<S>, Arc<RwLock<State<S>>>),
}

impl<S> Handle<S>
where
    S: 'static + AsSourceState,
{
    pub fn cache(&self) -> Arc<RwLock<State<S>>> {
        match self {
            Handle::Source(_, c) => c.clone(),
            Handle::Reader(_, c) => c.clone(),
        }
    }

    pub fn sender(&self) -> Result<MAsyncTx<List<StateEvent<S>>>, StateChangeError<S>> {
        match self {
            Handle::Source(source, _) => Ok(source.sender.clone()),
            Handle::Reader(_, _) => Err(StateChangeError::StateReadOnly),
        }
    }

    pub fn recver(&self) -> &MAsyncRx<List<StateEvent<S>>> {
        match self {
            Handle::Source(source, _) => &source.recver,
            Handle::Reader(reader, _) => &reader.recver,
        }
    }

    #[instrument(level = "trace", skip(self, f))]
    async fn inner_change(
        &self,
        f: impl FnOnce(S) -> S,
        is_touch: bool,
        wait_arrival: bool,
    ) -> Result<(), StateChangeError<S>> {
        let cache = self.cache();
        let mut guard = cache.write().await;
        let s_old = (*guard).value.clone();
        let s = f(s_old.clone());
        if is_touch || s_old != s {
            let (event, wait_rx) = if wait_arrival {
                let (tx, rx): (CloseHandle<mpmc::Null>, MAsyncRx<mpmc::Null>) =
                    mpmc::Null::new().new_async();
                let event = StateEvent {
                    state: State {
                        value: s,
                        timestamp: Utc::now(),
                    },
                    is_touch,
                    close_handle: Some(tx),
                };
                (event, Some(rx))
            } else {
                (
                    StateEvent {
                        state: State {
                            value: s,
                            timestamp: Utc::now(),
                        },
                        is_touch,
                        close_handle: None,
                    },
                    None,
                )
            };
            self.sender()?.send(event.clone()).await?;
            *guard = event.state.clone();
            tracing::debug!("send -- {:?}", event);
            if let Some(rx) = wait_rx {
                rx.recv().await?;
                tracing::debug!("done -- {:?}", event);
            }
        }
        Ok(())
    }
}

impl<S> Handle<S>
where
    S: 'static + AsSourceState,
{
    pub fn reader(&self) -> Reader<S> {
        match self {
            Handle::Source(source, _) => source.reader(),
            Handle::Reader(reader, _) => reader.clone(),
        }
    }

    pub async fn recv(&self) -> Result<Option<(State<S>, State<S>)>, RecvError> {
        let res = self.recver().recv().await;
        match res {
            Ok(s) => {
                let cache = self.cache();
                let s_old = { cache.read().await.clone() };
                if s.is_touch || s.state.value != s_old.value {
                    tracing::debug!("recv -- {:?}", s);
                    {
                        *cache.write().await = s.state.clone();
                    }
                    Ok(Some((s.state, s_old)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn value(&self) -> S {
        self.cache().read().await.value.clone()
    }

    pub async fn state(&self) -> State<S> {
        self.cache().read().await.clone()
    }

    pub async fn touch(&self) -> Result<(), StateChangeError<S>> {
        self.inner_change(|s| s.clone(), false, false).await
    }

    pub async fn wait_touch(&self) -> Result<(), StateChangeError<S>> {
        self.inner_change(|s| s.clone(), false, true).await
    }

    pub async fn alter(&self, s: S) -> Result<(), StateChangeError<S>> {
        self.inner_change(|_| s, true, false).await
    }

    pub async fn wait_alter(&self, s: S) -> Result<(), StateChangeError<S>> {
        self.inner_change(|_| s, true, true).await
    }

    pub async fn amend(&self, f: impl FnOnce(S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, true, false).await
    }

    pub async fn wait_amend(&self, f: impl FnOnce(S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, true, false).await
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
