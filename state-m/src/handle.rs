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
    sync::{Arc, OnceLock},
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
    Source(Source<S>, Arc<ArcSwap<S>>, Arc<ArcSwap<State<S>>>),
    Reader(Reader<S>, Arc<ArcSwap<State<S>>>),
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
            HandleI::Source(ref source, _, _) => source.sender.subscribe(),
            HandleI::Reader(ref reader, _) => reader.sender.subscribe(),
        }
    }

    fn pass_checks(&self) -> Arc<Vec<Arc<dyn AsPassCheck + Send + Sync>>> {
        match self.inner {
            HandleI::Source(ref source, _, _) => source.pass_checks.clone(),
            HandleI::Reader(ref reader, _) => reader.pass_checks.clone(),
        }
    }

    pub fn cache_r(&self) -> Arc<ArcSwap<State<T::Value>>> {
        match self.inner {
            HandleI::Source(_, _, ref cache_r) => cache_r.clone(),
            HandleI::Reader(_, ref cache_r) => cache_r.clone(),
        }
    }

    async fn inner_change(
        &self,
        f: impl FnOnce(T::Value) -> T::Value,
        is_touch: bool,
        wait_arrival: bool,
        to_cmp: Option<T::Value>,
    ) -> Result<(), StateChangeError<T::Value>> {
        match self.inner {
            HandleI::Source(ref source, ref cache_w, _) => {
                let sm_id = &self.sm_id;
                let tag = &self.tag;
                for check in source.pass_checks.iter() {
                    if !check.is_open() {
                        tracing::debug!("{sm_id} | {tag:?} | wait pass_check -- {check:?}");
                        check.notified().await;
                    }
                }
                let res = {
                    let v_old = cache_w.load().as_ref().clone();
                    let v = f(v_old.clone());
                    if let Some(v_cmp) = to_cmp
                        && v_cmp != v_old
                    {
                        return Err(StateChangeError::CompareFailure(
                            v_cmp.clone(),
                            v.clone(),
                            v_old.clone(),
                        ));
                    }
                    if is_touch || v_old != v {
                        let (event, wait_rx) = {
                            let state = State {
                                value: v.clone(),
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
                        cache_w.store(Arc::new(v));
                        match source.sender.send(event) {
                            Ok(recver_count) => {
                                tracing::debug!(
                                    "{sm_id} | {tag:?} | send | {recver_count} -- {state:?}"
                                );
                                (state, wait_rx)
                            }
                            Err(e) => {
                                cache_w.store(Arc::new(v_old));
                                return Err(e.into());
                            }
                        }
                    } else {
                        return Ok(());
                    }
                };
                if let (state, Some(mut rx)) = res {
                    _ = rx.recv().await;
                    tracing::debug!("{sm_id} | {tag:?} | done -- {state:?}");
                } else {
                    tokio::task::yield_now().await;
                }
            }
            HandleI::Reader(_, _) => return Err(StateChangeError::StateReadOnly),
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
            HandleI::Source(_, _, _) => true,
            HandleI::Reader(_, _) => false,
        }
    }

    pub fn capacity(&self) -> usize {
        match self.inner {
            HandleI::Source(ref source, _, _) => source.capacity,
            HandleI::Reader(ref reader, _) => reader.capacity,
        }
    }

    pub fn from_source(sm_id: String, tag: T, source: Source<T::Value>) -> Self {
        Self {
            sm_id,
            tag,
            inner: HandleI::Source(source, Default::default(), Default::default()),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn from_reader(sm_id: String, tag: T, reader: Reader<T::Value>) -> Self {
        Self {
            sm_id,
            tag,
            inner: HandleI::Reader(reader, Default::default()),
            cancel_token: Default::default(),
            fanout_tx: OnceLock::new(),
        }
    }

    pub fn reader(&self) -> Reader<T::Value> {
        match self.inner {
            HandleI::Source(ref source, _, _) => source.reader(),
            HandleI::Reader(ref reader, _) => reader.clone(),
        }
    }

    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    pub fn value(&self) -> T::Value {
        self.cache_r().load().value.clone()
    }

    pub fn state(&self) -> State<T::Value> {
        self.cache_r().load().as_ref().clone()
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
        let cache_r = self.cache_r();
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
                                let s_old = cache_r.load().as_ref().clone();
                                let s_new = e.state.clone();
                                if e.is_touch || s_new.value != s_old.value {
                                    for check in pass_checks.iter() {
                                        if !check.is_open() {
                                            tracing::debug!("{sm_id} | {tag:?} | wait pass_check -- {check:?}");
                                            check.notified().await;
                                        }
                                    }
                                    tracing::debug!("{sm_id} | {tag:?} | recv -- {s_new:?}");
                                    cache_r.store(Arc::new(s_new.clone()));
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
    S: Debug + Default,
{
    #[error("This state is read only.")]
    StateReadOnly,
    #[error("Compare failure, change from {_0:?} to {_1:?}, but current value is {_2:?}")]
    CompareFailure(S, S, S),
    #[error(transparent)]
    SendError(#[from] SendError<StateEvent<S>>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
