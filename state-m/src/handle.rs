use crate::{
    AsState,
    reader::Reader,
    source::Source,
    state::{State, StateEvent},
};
use chrono::Utc;
use downcast_rs::{Downcast, impl_downcast};
use std::{
    fmt::Debug,
    ops::Deref,
    sync::{Arc, OnceLock, RwLock},
};
use thiserror::Error;
use tokio::sync::{
    // RwLock,
    broadcast::{Sender, channel},
    mpsc,
};
use tokio::{
    select,
    sync::broadcast::{
        Receiver,
        error::{RecvError, SendError},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub trait AsHandle: Debug + Downcast + Send + Sync {
    fn debug_state(&self) -> Box<dyn Debug>;
}

impl_downcast!(AsHandle);

#[derive(Clone, Debug)]
pub(crate) struct ArcHandle<S>(pub Arc<Handle<S>>)
where
    S: 'static + AsState + Send + Sync;

impl<S> Deref for ArcHandle<S>
where
    S: 'static + AsState + Send + Sync,
{
    type Target = Handle<S>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<S> AsHandle for ArcHandle<S>
where
    S: 'static + AsState + Send + Sync,
{
    fn debug_state(&self) -> Box<dyn Debug> {
        Box::new(self.0.state())
    }
}

#[derive(Debug)]
enum HandleI<S>
where
    S: 'static + AsState,
{
    Source(Source<S>, Arc<RwLock<State<S>>>),
    Reader(Reader<S>),
}

#[derive(Debug)]
pub(crate) struct Handle<S>
where
    S: 'static + AsState,
{
    inner: HandleI<S>,
    cache: Arc<RwLock<State<S>>>,
    cancel_token: CancellationToken,
    fanout_tx: OnceLock<Sender<(State<S>, State<S>)>>,
}

impl<S> Drop for Handle<S>
where
    S: 'static + AsState,
{
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

impl<S> Handle<S>
where
    S: 'static + AsState,
{
    async fn recver(&self) -> Receiver<StateEvent<S>> {
        match self.inner {
            HandleI::Source(ref source, _) => source.sender.subscribe(),
            HandleI::Reader(ref reader) => reader.sender.subscribe(),
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
                let res = {
                    let mut guard = cache.write().unwrap();
                    let s_old = (*guard).value.clone();
                    let s = f(s_old.clone());
                    if is_touch || s_old != s {
                        let (event, wait_rx) = {
                            let state = State {
                                value: s,
                                timestamp: Utc::now(),
                            };
                            if wait_arrival {
                                let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) =
                                    mpsc::channel(1);
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
                        Some((state, wait_rx))
                    } else {
                        None
                    }
                };
                if let Some((state, Some(mut rx))) = res {
                    _ = rx.recv().await;
                    tracing::debug!("done -- {state:?}");
                } else {
                    tokio::task::yield_now().await;
                }
            }
            HandleI::Reader(_) => return Err(StateChangeError::StateReadOnly),
        }
        Ok(())
    }
}

impl<S> Handle<S>
where
    S: 'static + AsState,
{
    pub fn capacity(&self) -> usize {
        match self.inner {
            HandleI::Source(ref source, _) => source.capacity,
            HandleI::Reader(ref reader) => reader.capacity,
        }
    }

    pub fn from_source(source: Source<S>) -> Self {
        Self {
            inner: HandleI::Source(source, Default::default()),
            cache: Default::default(),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn from_reader(reader: Reader<S>) -> Self {
        Self {
            inner: HandleI::Reader(reader),
            cache: Default::default(),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn reader(&self) -> Reader<S> {
        match self.inner {
            HandleI::Source(ref source, _) => source.reader(),
            HandleI::Reader(ref reader) => reader.clone(),
        }
    }

    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    pub fn value(&self) -> S {
        self.cache.read().unwrap().value.clone()
    }

    pub fn state(&self) -> State<S> {
        self.cache.read().unwrap().clone()
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

    pub fn fanout(&self) -> (Receiver<(State<S>, State<S>)>, CancellationToken) {
        let rx = self
            .fanout_tx
            .get()
            .expect("The field 'fanout_tx' should have been set.")
            .subscribe();
        let token = self.cancel_token.child_token();
        (rx, token)
    }
}

impl<S> Handle<S>
where
    S: 'static + AsState + Send + Sync,
{
    pub async fn init<T>(&self, tag: T)
    where
        T: 'static + Debug + Send,
    {
        let cache = self.cache.clone();
        let cancel_token = self.cancel_token.clone();
        let mut recver = self.recver().await;
        let (fanout_tx, mut fanout_rx) = channel(self.capacity());
        self.fanout_tx
            .set(fanout_tx.clone())
            .expect("The 'init' method can only be called once.");
        tokio::spawn(async move {
            tracing::info!("init | {tag:?} -- start");
            loop {
                select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    r = fanout_rx.recv() => {
                        if r.is_err() {
                            break;
                        }
                    },
                    r = recver.recv() => {
                        match r {
                            Ok(e) => {
                                let s_old = { cache.read().unwrap().clone() };
                                let s_new = e.state.clone();
                                if e.is_touch || s_new.value != s_old.value {
                                    tracing::debug!("{tag:?} | recv -- {s_new:?}");
                                    {
                                        *cache.write().unwrap() = s_new.clone();
                                    }
                                    if fanout_tx.send((s_new, s_old)).is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            tracing::info!("init | {tag:?} -- close");
        });
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
