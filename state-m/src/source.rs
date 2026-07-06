use crate::{
    core::AsyncStreamWrapper,
    reader::Reader,
    state::{State, StateEvent},
};
use chrono::{DateTime, Utc};
use crossfire::{
    MAsyncRx, MAsyncTx, RecvError, SendError,
    mpmc::{self, List},
    null::CloseHandle,
};
use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};
use thiserror::Error;

#[derive(Clone)]
pub struct Source<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    cache: Arc<RwLock<State<S>>>,
    sender: MAsyncTx<List<StateEvent<S>>>,
    recver: MAsyncRx<List<StateEvent<S>>>,
}

impl<S> Default for Source<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Source<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    async fn inner_change(
        &self,
        f: impl FnOnce(&S) -> S,
        is_touch: bool,
        wait_arrival: bool,
    ) -> Result<(), StateChangeError<S>> {
        let mut guard = self.cache.write().unwrap();
        let s_old = (*guard).value.clone();
        let s = f(&s_old);
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
            let state = event.state.clone();
            self.sender.send(event).await?;
            *guard = state;
            if let Some(rx) = wait_rx {
                rx.recv().await?;
            }
            Ok(())
        } else {
            Err(StateChangeError::NotChange)
        }
    }
}

impl<S> Source<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    pub fn new() -> Self {
        let (tx, rx) = mpmc::unbounded_async();
        Self {
            cache: Arc::new(RwLock::new(State::default())),
            sender: tx.into_async(),
            recver: rx,
        }
    }

    pub fn reader(&self) -> Reader<S> {
        Reader {
            stream: AsyncStreamWrapper(self.recver.clone().into_stream()),
        }
    }

    pub fn value(&self) -> S {
        self.cache.read().unwrap().value.clone()
    }

    pub fn value_ex(&self) -> (S, DateTime<Utc>) {
        (
            self.cache.read().unwrap().value.clone(),
            self.cache.read().unwrap().timestamp.clone(),
        )
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

    pub async fn amend(&self, f: impl FnOnce(&S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, true, false).await
    }

    pub async fn wait_amend(&self, f: impl FnOnce(&S) -> S) -> Result<(), StateChangeError<S>> {
        self.inner_change(f, true, false).await
    }
}

#[derive(Debug, Error)]
pub enum StateChangeError<S>
where
    S: Default,
{
    #[error("state not change")]
    NotChange,
    #[error(transparent)]
    SendError(#[from] SendError<StateEvent<S>>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
