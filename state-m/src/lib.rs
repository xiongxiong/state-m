use async_trait::async_trait;
#[cfg(feature = "timestamp")]
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    any::{Any, type_name},
    cmp::Eq,
    fmt::Debug,
    hash::Hash,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    select,
    sync::{RwLock, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

/// State machine data structure to store sources and handles.
/// - G - to distinguish different initiators or responders.
#[derive(Clone, Debug)]
pub struct StateMachine<G>
where
    G: Eq + Hash,
{
    sources: Arc<DashMap<G, Box<dyn Any + Send + Sync>>>,
    handles: Arc<DashMap<G, Box<dyn Any + Send + Sync>>>,
}

impl<G> Default for StateMachine<G>
where
    G: Eq + Hash,
{
    fn default() -> Self {
        Self {
            sources: Default::default(),
            handles: Default::default(),
        }
    }
}

impl<G> StateMachine<G>
where
    G: Clone + Debug + Eq + Hash,
{
    pub fn new() -> Self {
        Default::default()
    }

    /// Add source to state machine.
    fn add_source<S>(&self, tag: G, source: Source<S>)
    where
        S: 'static + Send + Sync,
    {
        assert!(
            !self.sources.contains_key(&tag),
            "Source already exist, tag -- {:?}, type -- {:?}",
            tag,
            type_name::<S>()
        );
        self.sources.insert(tag, Box::new(source));
    }

    /// Delete source from state machine.
    fn del_source(&self, tag: &G) -> bool {
        self.sources.remove(tag).is_some()
    }

    /// If source of tag exists in state machine.
    fn has_source(&self, tag: &G) -> bool {
        self.sources.contains_key(tag)
    }

    /// Get source from state machine by tag, panics if source of tag doesn't exist or data type of source is wrong.
    async fn source<S>(&self, tag: &G) -> Source<S>
    where
        S: 'static + Clone,
    {
        let opt_source_box = self.sources.get(tag);
        assert!(
            opt_source_box.is_some(),
            "source does not exist, tag -- {:?}",
            tag
        );
        let source_box = opt_source_box.unwrap();
        let opt_source = source_box.downcast_ref::<Source<S>>();
        assert!(
            opt_source.is_some(),
            "source does not exist, tag -- {:?}, type -- {}",
            tag,
            type_name::<S>()
        );
        let source = opt_source.unwrap();
        (*source).clone()
    }

    /// Get current value of source from state machine by tag.
    async fn source_value<S>(&self, tag: &G) -> S
    where
        S: 'static + Clone + Default + PartialEq + Send,
    {
        self.source(tag).await.value().await
    }

    /// Get current value of source with timestamp from state machine by tag.
    async fn source_value_ex<S>(&self, tag: &G) -> Value<S>
    where
        S: 'static + Clone + Default + PartialEq + Send,
    {
        self.source(tag).await.value_ex().await
    }

    /// Add handle to state machine.
    fn add_handle<T>(&self, tag: G, handle: Handle<T>)
    where
        T: 'static + Send + Sync,
    {
        assert!(
            !self.handles.contains_key(&tag),
            "duplicate tag for handle -- {:?}",
            tag
        );
        self.handles.insert(tag, Box::new(handle));
    }

    /// Delete handle from state machine.
    fn del_handle(&self, tag: &G) -> bool {
        self.handles.remove(tag).is_some()
    }

    /// If handle of tag exists in state machine.
    fn has_handle(&self, tag: &G) -> bool {
        self.handles.contains_key(tag)
    }

    /// Get handle from state machine, panics if handle of tag doesn't exist or data type of handle is wrong.
    async fn handle<T>(&self, tag: &G) -> Handle<T>
    where
        T: 'static + Clone,
    {
        let opt_handle_box = self.handles.get(tag);
        assert!(
            opt_handle_box.is_some(),
            "handle does not exist, tag -- {:?}",
            tag
        );
        let handle_box = opt_handle_box.unwrap();
        let opt_handle = handle_box.downcast_ref::<Handle<T>>();
        assert!(
            opt_handle.is_some(),
            "handle does not exist, tag -- {:?}, type -- {}",
            tag,
            type_name::<T>()
        );
        opt_handle.unwrap().clone()
    }

    /// Get current value of handle from state machine.
    async fn handle_value<T>(&self, tag: &G) -> T
    where
        T: 'static + Clone + PartialEq,
    {
        self.handle(tag).await.value().await
    }

    /// Get current value of handle with timestamp from state machine.
    async fn handle_value_ex<T>(&self, tag: &G) -> Value<T>
    where
        T: 'static + Clone + PartialEq,
    {
        self.handle(tag).await.value_ex().await
    }
}

/// At least you should provide a state machine data structure.
#[async_trait]
pub trait HasStateMachine<G>
where
    G: Clone + Debug + Eq + Hash,
{
    /// The state machine data structure.
    async fn state_machine(&self) -> StateMachine<G>;
}

/// Some convenient methods to use state machine. The trait is auto implemented for types implemented HasStateMachine.
#[async_trait]
pub trait UseStateMachine<G>: HasStateMachine<G>
where
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Add source to state machine, the source is created by default.
    async fn add_source<S>(&self, tag: G)
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .add_source(tag, Source::<S>::default());
    }

    /// Add source to state machine.
    async fn add_source_ex<S>(&self, tag: G, chan_capacity: usize, init_value: S)
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .add_source(tag, Source::create(init_value, chan_capacity));
    }

    /// Delete source from state machine.
    async fn del_source(&self, tag: &G) -> bool {
        self.state_machine().await.del_source(tag)
    }

    /// If source of tag exists in state machine.
    async fn has_source(&self, tag: &G) -> bool {
        self.state_machine().await.has_source(tag)
    }

    /// Num of subscriptions.
    async fn num_of_subscriptions<S>(&self, tag: &G) -> usize
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .source::<S>(tag)
            .await
            .num_of_subscriptions()
            .await
    }

    /// Get current value of source.
    async fn source_value<S>(&self, tag: &G) -> S
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine().await.source_value(tag).await
    }

    /// Get current value of source with timestamp.
    async fn source_value_ex<S>(&self, tag: &G) -> Value<S>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine().await.source_value_ex(tag).await
    }

    /// Change state of source.
    async fn change<S>(&self, tag: &G, s: S) -> Result<(), SourceChangeError>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine().await.source(tag).await.change(s).await
    }

    /// Change state of source, and wait responders to finish actions upon the change event.
    async fn wait_change<S>(&self, tag: &G, s: S) -> Result<(), SourceChangeError>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .source(tag)
            .await
            .wait_change(s)
            .await
    }

    /// Change state of source by modifying it with a func.
    async fn modify<S>(
        &self,
        tag: &G,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .source(tag)
            .await
            .modify(func)
            .await
    }

    /// Change state of source by modifying it with a func, and wait responders to finish actions upon the change event.
    async fn wait_modify<S>(
        &self,
        tag: &G,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .source(tag)
            .await
            .wait_modify(func)
            .await
    }

    /// Create a change event without changing state of source really.
    async fn touch<S>(&self, tag: &G) -> Result<(), SourceChangeError>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .source::<S>(tag)
            .await
            .touch()
            .await
    }

    /// If handle of tag exists in state machine.
    async fn has_handle(&self, tag: &G) -> bool {
        self.state_machine().await.has_handle(tag)
    }

    /// Get current value of handle.
    async fn handle_value<T>(&self, tag: &G) -> T
    where
        T: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine().await.handle_value(&tag).await
    }

    /// Get current value of handle with timestamp.
    async fn handle_value_ex<T>(&self, tag: &G) -> Value<T>
    where
        T: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine().await.handle_value_ex(&tag).await
    }

    /// Get reader of source, can be subscribed by responders.
    async fn reader<S>(&self, tag: &G) -> Reader<S>
    where
        S: 'static + Clone + Default + PartialEq + Send,
    {
        self.state_machine().await.source::<S>(tag).await.reader()
    }

    /// Get reader of source, can be subscribed by responders.
    async fn reader_ex<S, T>(
        &self,
        tag: &G,
        func: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, T>
    where
        S: 'static + Clone + Default + PartialEq + Send,
    {
        self.state_machine()
            .await
            .source::<S>(tag)
            .await
            .reader_ex(func)
    }

    /// Unsubscription
    async fn unsubscribe<T>(&self, tag: &G)
    where
        T: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine()
            .await
            .handle::<T>(tag)
            .await
            .unsubscribe();
    }
}

#[async_trait]
impl<T, G> UseStateMachine<G> for T
where
    T: HasStateMachine<G>,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

/// When initiate state change, compare with current value or not. By default,
/// a new state is compared with current value, if they are equal, does not trigger a change event.
type NotCheckEq = bool;

#[cfg(feature = "timestamp")]
pub type Value<S> = (S, DateTime<Utc>);

#[cfg(not(feature = "timestamp"))]
pub type Value<S> = S;

/// source, the initiator of state change.
#[derive(Clone, Debug)]
struct Source<S> {
    value: Arc<RwLock<Value<S>>>,
    sender: broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
}

impl<S> Default for Source<S>
where
    S: 'static + Clone + Default + PartialEq + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Source<S>
where
    S: 'static + Clone + Default + PartialEq + Send,
{
    /// Create a source, with broadcast channel capacity of 100.
    fn new() -> Self {
        Self::create(Default::default(), 100)
    }

    /// Create a source with custom broadcast channel capacity.
    /// - chan_capacity: broadcast channel capacity
    fn create(init_value: S, chan_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(chan_capacity);
        #[cfg(feature = "timestamp")]
        let v = (init_value, Utc::now());
        #[cfg(not(feature = "timestamp"))]
        let v = init_value;
        Self {
            value: Arc::new(RwLock::new(v)),
            sender: tx,
        }
    }

    /// Get reader of source, can be subscribed by responders.
    fn reader(&self) -> Reader<S> {
        Reader {
            value: self.value.clone(),
            recver: self.sender.subscribe(),
        }
    }

    /// Get reader of source, can be subscribed by responders.
    fn reader_ex<T>(
        &self,
        func: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, T> {
        ReaderEx {
            value: self.value.clone(),
            recver: self.sender.subscribe(),
            func: Arc::new(func),
        }
    }

    /// Num of subscriptions.
    async fn num_of_subscriptions(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Get current value of source.
    async fn value(&self) -> S {
        #[cfg(feature = "timestamp")]
        {
            (*self.value.read().await).clone().0
        }
        #[cfg(not(feature = "timestamp"))]
        {
            (*self.value.read().await).clone()
        }
    }

    /// Get current value with timestamp of source.
    async fn value_ex(&self) -> Value<S> {
        (*self.value.read().await).clone()
    }

    async fn change_ex(
        &self,
        wait_to_end: bool,
        change: Change<S>,
    ) -> Result<(), SourceChangeError> {
        let mut guard = self.value.write().await;
        #[cfg(feature = "timestamp")]
        let g = (*guard).0.clone();
        #[cfg(not(feature = "timestamp"))]
        let g = (*guard).clone();
        let (s, not_check_eq) = match change {
            Change::Value(v) => (v, false),
            Change::Func(func) => (func(g.clone()), false),
            Change::Touch => (g.clone(), true),
        };
        if not_check_eq || g != s {
            if wait_to_end {
                let (tx_w, mut rx_w) = mpsc::unbounded_channel::<()>();
                self.sender
                    .send((s.clone(), not_check_eq, Some(tx_w)))
                    .map_err(|_| SourceChangeError::SendErr)?;
                loop {
                    select! {
                        res = rx_w.recv()  => {
                            if res.is_none() {
                                break;
                            }
                        }
                    }
                }
            } else {
                self.sender
                    .send((s.clone(), not_check_eq, None))
                    .map_err(|_| SourceChangeError::SendErr)?;
            }
            #[cfg(feature = "timestamp")]
            {
                *guard = (s, Utc::now());
            }
            #[cfg(not(feature = "timestamp"))]
            {
                *guard = s;
            }
            Ok(())
        } else {
            Err(SourceChangeError::NotChange)
        }
    }

    /// Change state of source.
    async fn change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Value(s)).await
    }

    /// Change state of source, and wait responders to finish actions upon the change event.
    async fn wait_change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Value(s)).await
    }

    /// Change state of source by modifying it with a func.
    async fn modify(
        &self,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Func(Arc::new(func))).await
    }

    /// Change state of source by modifying it with a func, and wait responders to finish actions upon the change event.
    async fn wait_modify(
        &self,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Func(Arc::new(func))).await
    }

    /// Create a change event without changing state of source really.
    async fn touch(&self) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Touch).await
    }
}

enum Change<S> {
    Value(S),
    Func(Arc<dyn Fn(S) -> S + Send + Sync>),
    Touch,
}

#[derive(Debug, Error)]
pub enum SourceChangeError {
    #[error("Change of state failed to broadcast")]
    SendErr,
    #[error("source not change, no change detected")]
    NotChange,
}

/// Data structure to be exposed to do subscription by state change responders.
pub struct Reader<S> {
    value: Arc<RwLock<Value<S>>>,
    recver: broadcast::Receiver<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
}

impl<S> Into<ReaderEx<S, S>> for Reader<S>
where
    S: 'static + Send,
{
    fn into(self) -> ReaderEx<S, S> {
        ReaderEx {
            value: self.value,
            recver: self.recver,
            func: Arc::new(|s| Box::pin(async move { s })),
        }
    }
}

impl<S> Reader<S> {
    pub fn extend<T>(
        self,
        func: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, T> {
        ReaderEx {
            value: self.value,
            recver: self.recver,
            func: Arc::new(func),
        }
    }
}

/// Data structure to be exposed to do subscription by state change responders, with the ability to convert the state to another type.
pub struct ReaderEx<S, T> {
    value: Arc<RwLock<Value<S>>>,
    recver: broadcast::Receiver<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
    func: Arc<dyn Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync>,
}

impl<S, T> ReaderEx<S, T>
where
    S: 'static + Clone + Send,
    T: 'static,
{
    async fn value(&self) -> Value<T> {
        #[cfg(feature = "timestamp")]
        {
            let (s, t) = (*self.value.read().await).clone();
            (self.func.as_ref()(s).await, t)
        }
        #[cfg(not(feature = "timestamp"))]
        {
            self.func.as_ref()((*self.value.read().await).clone()).await
        }
    }

    pub fn extend<U>(
        self,
        func: impl Fn(T) -> Pin<Box<dyn Future<Output = U> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, U> {
        let func_o = self.func.clone();
        let func_n = Arc::new(func);
        ReaderEx {
            value: self.value,
            recver: self.recver,
            func: Arc::new(move |s| {
                let func_a = func_o.clone();
                let func_b = func_n.clone();
                Box::pin(async move {
                    let t = func_a.as_ref()(s).await;
                    func_b.as_ref()(t).await
                })
            }),
        }
    }
}

/// Data structure to store the latest state in responder's state machine, can be used to do unsubscription.
#[derive(Clone, Debug)]
struct Handle<T> {
    cancel_token: CancellationToken,
    value: Arc<RwLock<Value<T>>>,
}

impl<T> Handle<T>
where
    T: Clone + PartialEq,
{
    fn new(init_value: T) -> Self {
        #[cfg(feature = "timestamp")]
        let t = (init_value, Utc::now());
        #[cfg(not(feature = "timestamp"))]
        let t = init_value;
        Self {
            cancel_token: CancellationToken::new(),
            value: Arc::new(RwLock::new(t)),
        }
    }

    async fn store(&self, t: T, not_check_eq: bool) -> bool {
        #[cfg(feature = "timestamp")]
        let v = (t, Utc::now());
        #[cfg(not(feature = "timestamp"))]
        let v = t;
        let changed = *self.value.read().await != v;
        if changed {
            *self.value.write().await = v;
        }
        not_check_eq || changed
    }

    async fn value(&self) -> T {
        #[cfg(feature = "timestamp")]
        {
            (*self.value.read().await).clone().0
        }
        #[cfg(not(feature = "timestamp"))]
        {
            (*self.value.read().await).clone()
        }
    }

    async fn value_ex(&self) -> Value<T> {
        (*self.value.read().await).clone()
    }

    /// Unsubscription, this is optional, after your state machine
    /// is dropped, subscriptions are auto cleaned.
    fn unsubscribe(&self) {
        self.cancel_token.cancel();
    }
}

/// Define action upon state change event.
/// - T - type of state in handle,
/// - G - to distinguish different initiators or responders,
/// all initiators must use different tag values, all responders,
/// and all responders do the same, a same tag value can be used
/// by an initiator and a responder in the same state machine.
#[async_trait]
pub trait HasStateHandle<T, G>: HasStateMachine<G>
where
    T: Clone + Debug + PartialEq,
    G: Clone + Debug + Eq + Hash,
{
    /// Action upon state change event.
    /// - tag - the tag value
    /// - new_value - the new value just received
    /// - old_value - the value received last time, it should be
    /// 'None' at the first time.
    async fn on_change(
        self: Arc<Self>,
        tag: G,
        new_value: T,
        old_value: T,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

/// Convenient method to do subscription with a state convert function. The trait is auto implemented for types implemented HasStateHandle.
#[async_trait]
pub trait UseStateHandle<T, G>: HasStateHandle<T, G> + 'static
where
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Do subscription with a state convert function.
    /// - stage [1] -- receive from source's broadcast channel.
    /// - stage [3] -- receive from source's broadcast channel and process it.
    /// - stage [4] -- (optional) feedback when the change event has been processed.
    #[instrument(name = "UseStateHandle::subscribe", skip_all, fields(tag))]
    async fn subscribe<S>(self: Arc<Self>, reader: impl Into<ReaderEx<S, T>> + Send, tag: G)
    where
        S: 'static + Clone + Debug + PartialEq + Send + Sync,
    {
        let reader_ex = reader.into();
        #[cfg(feature = "timestamp")]
        let init = reader_ex.value().await.0;
        #[cfg(not(feature = "timestamp"))]
        let init = reader_ex.value().await;
        let handle: Handle<T> = Handle::new(init);
        self.state_machine()
            .await
            .add_handle(tag.clone(), handle.clone());
        let mut rx_s = reader_ex.recver;
        tokio::spawn(async move {
            tracing::info!("Subscription start -- {:?}", tag);
            loop {
                select! {
                    _ = handle.cancel_token.cancelled() => {
                        break;
                    }
                    res = rx_s.recv() => {
                        match res {
                            Ok((s, not_check_eq, opt_feedback)) => {
                                let v = reader_ex.func.as_ref()(s).await;
                                let t_old = handle.value().await;
                                if handle.store(v.clone(), not_check_eq).await {
                                    let t_new = handle.value().await;
                                    if let Err(e) = self.clone().on_change(tag.clone(), t_new, t_old).await {
                                        tracing::error!("stage [2] | change event proc error -- {}", e);
                                    }
                                    if let Some(feedback) = opt_feedback && let Err(e) = feedback.send(()) {
                                        tracing::error!("stage [3] | change event feedback error -- {}", e);
                                    }
                                }
                            },
                            Err(e) => match e {
                                broadcast::error::RecvError::Closed => {
                                    _ = self.state_machine().await.del_source(&tag);
                                    tracing::info!("source channel closed");
                                    break;
                                },
                                broadcast::error::RecvError::Lagged(_) => {
                                    tracing::error!("stage [1] | change event recv lagged");
                                    break;
                                },
                            },
                        }
                    }
                }
            }
            _ = self.state_machine().await.del_handle(&tag);
            tracing::info!("Subscription end -- {:?}", tag);
        });
    }
}

impl<V, T, G> UseStateHandle<T, G> for V
where
    V: 'static + HasStateHandle<T, G>,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}
