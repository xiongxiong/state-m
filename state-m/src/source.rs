use crate::{
    core::AsyncStreamWrapper,
    reader::Reader,
    state::{State, StateEvent},
};
use chrono::{DateTime, Utc};
use crossfire::{
    MAsyncRx, MAsyncTx,
    mpmc::{self, List},
    null::CloseHandle,
};
use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

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

    async fn inner_change<F>(&self, change: Change<S>, wait_arrival: bool) {
        let mut guard = self.cache.write().unwrap();
        let s_old = (*guard).value.clone();
        let (s, is_touch) = match change {
            Change::Value(v) => (v, false),
            Change::Touch => (s_old.clone(), true),
        };
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
            *guard = event.state.clone();
            self.sender.send(event).await;
            if let Some(rx) = wait_rx {
                rx.recv().await;
            }
        }
    }
}

enum Change<S> {
    Touch,
    Value(S),
}
