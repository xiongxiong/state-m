use crate::{
    AsState, KvAssoc,
    barrier::AsPassCheck,
    reader::Reader,
    source::Source,
    state::{State, StateEvent},
};
use arc_swap::ArcSwap;
use chrono::Utc;
use downcast_rs::{Downcast, impl_downcast};
use std::{
    fmt::Debug,
    ops::Deref,
    sync::{Arc, OnceLock, RwLock},
};
use thiserror::Error;
use tokio::sync::{
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

pub trait AsHandle: Debug + Downcast + Send + Sync {
    fn debug_state(&self) -> Box<dyn Debug>;
}

impl_downcast!(AsHandle);

#[derive(Clone, Debug)]
pub(crate) struct ArcHandle<T>(pub Arc<Handle<T>>)
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState;

impl<T> Deref for ArcHandle<T>
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState,
{
    type Target = Handle<T>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T> AsHandle for ArcHandle<T>
where
    T: 'static + Clone + Debug + KvAssoc + Send + Sync,
    T::Value: AsState,
{
    fn debug_state(&self) -> Box<dyn Debug> {
        Box::new(self.0.state())
    }
}

#[derive(Debug)]
enum HandleI<S>
where
    S: AsState,
{
    Source(Source<S>, Arc<RwLock<State<S>>>),
    Reader(Reader<S>),
}

#[derive(Debug)]
pub(crate) struct Handle<T>
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState,
{
    sm_id: String,
    tag: T,
    inner: HandleI<T::Value>,
    cache: Arc<ArcSwap<State<T::Value>>>,
    cancel_token: CancellationToken,
    fanout_tx: OnceLock<Sender<(State<T::Value>, State<T::Value>)>>,
}

impl<T> Drop for Handle<T>
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState,
{
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

impl<T> Handle<T>
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState,
{
    fn recver(&self) -> Receiver<StateEvent<T::Value>> {
        match self.inner {
            HandleI::Source(ref source, _) => source.sender.subscribe(),
            HandleI::Reader(ref reader) => reader.sender.subscribe(),
        }
    }

    fn pass_checks(&self) -> Arc<Vec<Box<dyn AsPassCheck + Send + Sync>>> {
        match self.inner {
            HandleI::Source(ref source, _) => source.pass_checks.clone(),
            HandleI::Reader(ref reader) => reader.pass_checks.clone(),
        }
    }

    async fn inner_change(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
        is_touch: bool,
        wait_arrival: bool,
        pre_cmp: Option<T::Value>,
    ) -> Result<(), StateChangeError<T::Value>> {
        let sm_id = &self.sm_id;
        let tag = &self.tag;
        match self.inner {
            HandleI::Source(ref source, ref cache) => {
                for check in source.pass_checks.iter() {
                    if !check.is_open() {
                        tracing::trace!("{sm_id} | {tag:?} | wait pass_check -- {check:?}");
                        check.notified().await;
                    }
                }
                let res = {
                    let mut guard = cache.write().unwrap();
                    let s_old = (*guard).value.clone();
                    if let Some(s_cmp) = pre_cmp
                        && s_cmp != s_old
                    {
                        return Err(StateChangeError::CompareFailure);
                    }
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
                        tracing::trace!("{sm_id} | {tag:?} | {recver_count} | send -- {state:?}");
                        (state, wait_rx)
                    } else {
                        return Ok(());
                    }
                };
                if let (state, Some(mut rx)) = res {
                    _ = rx.recv().await;
                    tracing::trace!("{sm_id} | {tag:?} | done -- {state:?}");
                } else {
                    tokio::task::yield_now().await;
                }
            }
            HandleI::Reader(_) => return Err(StateChangeError::StateReadOnly),
        }
        Ok(())
    }
}

impl<T> Handle<T>
where
    T: Clone + Debug + KvAssoc,
    T::Value: AsState,
{
    pub fn is_source(&self) -> bool {
        match self.inner {
            HandleI::Source(_, _) => true,
            HandleI::Reader(_) => false,
        }
    }

    pub fn capacity(&self) -> usize {
        match self.inner {
            HandleI::Source(ref source, _) => source.capacity,
            HandleI::Reader(ref reader) => reader.capacity,
        }
    }

    pub fn from_source(sm_id: String, tag: T, source: Source<T::Value>) -> Self {
        Self {
            sm_id,
            tag,
            inner: HandleI::Source(source, Default::default()),
            cache: Default::default(),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn from_reader(sm_id: String, tag: T, reader: Reader<T::Value>) -> Self {
        Self {
            sm_id,
            tag,
            inner: HandleI::Reader(reader),
            cache: Default::default(),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn reader(&self) -> Reader<T::Value> {
        match self.inner {
            HandleI::Source(ref source, _) => source.reader(),
            HandleI::Reader(ref reader) => reader.clone(),
        }
    }

    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    pub fn value(&self) -> T::Value {
        self.cache.load().value.clone()
    }

    pub fn state(&self) -> State<T::Value> {
        self.cache.load().as_ref().clone()
    }

    pub async fn touch(&self) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|s| s.clone(), true, false, None).await
    }

    pub async fn wait_touch(&self) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|s| s.clone(), true, true, None).await
    }

    pub async fn alter(&self, s: T::Value) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|_| s, false, false, None).await
    }

    pub async fn cmp_alter(
        &self,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|_| s, false, false, Some(s_cmp)).await
    }

    pub async fn wait_alter(&self, s: T::Value) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|_| s, false, true, None).await
    }

    pub async fn wait_cmp_alter(
        &self,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(|_| s, false, true, Some(s_cmp)).await
    }

    pub async fn amend(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(f, false, false, None).await
    }

    pub async fn cmp_amend(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(f, false, false, Some(s_cmp)).await
    }

    pub async fn wait_amend(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(f, false, false, None).await
    }

    pub async fn wait_cmp_amend(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T::Value>> {
        self.inner_change(f, false, false, Some(s_cmp)).await
    }

    pub fn fanout(
        &self,
    ) -> (
        Receiver<(State<T::Value>, State<T::Value>)>,
        CancellationToken,
    ) {
        let rx = self
            .fanout_tx
            .get()
            .expect("The field 'fanout_tx' should have been set.")
            .subscribe();
        let token = self.cancel_token.child_token();
        (rx, token)
    }
}

impl<T> Handle<T>
where
    T: 'static + Clone + Debug + KvAssoc + Send,
    T::Value: AsState,
{
    pub async fn init(&self) {
        let sm_id = self.sm_id.clone();
        let tag = self.tag.clone();
        let cache = self.cache.clone();
        let cancel_token = self.cancel_token.clone();
        let pass_checks = self.pass_checks();
        let mut recver = self.recver();
        let (fanout_tx, mut fanout_rx) = channel(self.capacity());
        self.fanout_tx
            .set(fanout_tx.clone())
            .expect("The 'init' method can only be called once.");
        tokio::spawn(async move {
            tracing::info!("{sm_id} | {tag:?} | init -- start");
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
                                let s_old = { cache.load().as_ref().clone() };
                                let s_new = e.state.clone();
                                if e.is_touch || s_new.value != s_old.value {
                                    for check in pass_checks.iter() {
                                        if !check.is_open() {
                                            tracing::trace!("{sm_id} | {tag:?} | wait pass_check -- {check:?}");
                                            check.notified().await;
                                        }
                                    }
                                    tracing::trace!("{sm_id} | {tag:?} | recv -- {s_new:?}");
                                    {
                                        cache.store(Arc::new(s_new.clone()));
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
            tracing::info!("{sm_id} | {tag:?} | init -- close");
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
    #[error("This state used to compare is out of date.")]
    CompareFailure,
    #[error(transparent)]
    SendError(#[from] SendError<StateEvent<S>>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
