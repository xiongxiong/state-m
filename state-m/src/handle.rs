use crate::{
    AsState,
    reader::Reader,
    source::Source,
    state::{State, StateEvent},
};
use chrono::Utc;
use std::{
    fmt::Debug,
    sync::{Arc, OnceLock},
};
use thiserror::Error;
use tokio::sync::{
    RwLock,
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
    fn capacity(&self) -> usize {
        match self.inner {
            HandleI::Source(ref source, _) => source.capacity,
            HandleI::Reader(ref reader) => reader.capacity,
        }
    }

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
    S: 'static + AsState,
{
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
                                let s_old = { cache.read().await.clone() };
                                let s_new = e.state.clone();
                                if e.is_touch || s_new.value != s_old.value {
                                    tracing::debug!("{tag:?} | recv -- {s_new:?}");
                                    {
                                        *cache.write().await = s_new.clone();
                                    }
                                    _ = fanout_tx.send((s_new, s_old));
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
