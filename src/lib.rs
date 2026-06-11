use async_trait::async_trait;
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
    sync::{MutexGuard, RwLock, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

/// State machine data structure to store state sources and handles.
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

    /// Add state source to state machine.
    pub(crate) fn add_source<S>(&self, tag: G, source: Source<S>)
    where
        S: 'static + Send + Sync,
    {
        assert!(
            !self.sources.contains_key(&tag),
            "duplicate tag for source -- {:?}",
            tag
        );
        self.sources.insert(tag, Box::new(source));
    }

    /// Delete state source from state machine.
    pub(crate) fn del_source(&self, tag: G) -> bool {
        self.sources.remove(&tag).is_some()
    }

    /// Get source from state machine by tag.
    pub async fn source<S>(&self, tag: G) -> Source<S>
    where
        S: 'static + Clone,
    {
        let opt_source_box = self.sources.get(&tag);
        assert!(
            opt_source_box.is_some(),
            "state source does not exist, tag -- {:?}",
            tag
        );
        let source_box = opt_source_box.unwrap();
        let opt_source = source_box.downcast_ref::<Source<S>>();
        assert!(
            opt_source.is_some(),
            "state source does not exist, tag -- {:?}, type -- {}",
            tag,
            type_name::<S>()
        );
        let source = opt_source.unwrap();
        (*source).clone()
    }

    /// Add state handle to state machine.
    pub(crate) fn add_handle<T>(&self, tag: G, handle: Handle<T>)
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

    /// Delete state handle from state machine.
    pub(crate) fn del_handle(&self, tag: G) -> bool {
        self.handles.remove(&tag).is_some()
    }

    /// Get current value of source from state machine by tag.
    pub async fn source_value<S>(&self, tag: G) -> S
    where
        S: 'static + Clone + Default + PartialEq + Send,
    {
        self.source(tag).await.value().await
    }

    /// Get handle from state machine.
    pub async fn handle<T>(&self, tag: G) -> Handle<T>
    where
        T: 'static + Clone,
    {
        let opt_handle_box = self.handles.get(&tag);
        assert!(
            opt_handle_box.is_some(),
            "state handle does not exist, tag -- {:?}",
            tag
        );
        let handle_box = opt_handle_box.unwrap();
        let opt_handle = handle_box.downcast_ref::<Handle<T>>();
        assert!(
            opt_handle.is_some(),
            "state handle does not exist, tag -- {:?}, type -- {}",
            tag,
            type_name::<T>()
        );
        opt_handle.unwrap().clone()
    }

    /// Get current value of handle from state machine.
    pub async fn handle_value<T>(&self, tag: G) -> T
    where
        T: 'static + Clone + PartialEq,
    {
        self.handle(tag).await.value().await
    }
}

/// The data structure is locked while responding state change.
#[async_trait]
pub trait HasLock {
    /// The mutex lock to use.
    async fn lock(&self) -> MutexGuard<'_, ()>;
}

/// At least you should provide a state machine data structure.
#[async_trait]
pub trait HasStateMachine<G>: HasLock
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
    /// Get state source.
    async fn source<S>(&self, tag: G) -> Source<S>
    where
        S: 'static + Clone,
    {
        self.state_machine().await.source(tag).await
    }

    /// Get current value of state source.
    async fn source_value<S>(&self, tag: G) -> Option<S>
    where
        S: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine().await.source_value(tag).await
    }

    /// Get state handle.
    async fn handle<T>(&self, tag: G) -> Handle<T>
    where
        T: 'static + Clone,
    {
        self.state_machine().await.handle(tag).await
    }

    /// Get current value of state handle.
    async fn handle_value<T>(&self, tag: G) -> T
    where
        T: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine().await.handle_value(tag).await
    }
}

#[async_trait]
impl<T, G> UseStateMachine<G> for T
where
    T: HasStateMachine<G>,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

/// Convenient method to add state source to state machine. The trait is auto implemented for types implemented HasStateMachine.
#[async_trait]
pub trait UseStateSource<G>: HasStateMachine<G>
where
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Add state source to state machine, the state source is created by default.
    async fn add_source<S>(&self, tag: G) -> Source<S>
    where
        S: 'static + Clone + Default + PartialEq + Send + Sync,
    {
        let source = Source::<S>::default();
        self.state_machine().await.add_source(tag, source.clone());
        source
    }

    /// Add state source to state machine.
    async fn add_source_ex<S>(&self, tag: G, source: Source<S>) -> Source<S>
    where
        S: 'static + Clone + Send + Sync,
    {
        self.state_machine().await.add_source(tag, source.clone());
        source
    }
}

impl<T, G> UseStateSource<G> for T
where
    T: HasStateMachine<G>,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

/// When initiate state change, compare with current value or not. By default,
/// a new state is compared with current value, if they are equal, does not trigger a change event.
type NotCheckEq = bool;

/// State source, the initiator of state change.
#[derive(Clone, Debug)]
pub struct Source<S> {
    value: Arc<RwLock<S>>,
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
    /// Create a state source, with broadcast channel capacity of 100.
    pub fn new() -> Self {
        Self::create(Default::default(), 100)
    }
}

impl<S> Source<S>
where
    S: 'static + Clone + PartialEq + Send,
{
    /// Create a state source with custom broadcast channel capacity.
    /// - capacity: broadcast channel capacity
    pub fn create(init_value: S, capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            value: Arc::new(RwLock::new(init_value)),
            sender: tx,
        }
    }

    /// Get reader of state source, can be subscribed by responders.
    pub fn reader(&self) -> Reader<S> {
        Reader {
            value: self.value.clone(),
            sender: self.sender.clone(),
        }
    }

    /// Get reader of state source, can be subscribed by responders.
    pub fn reader_ex<T>(
        &self,
        func: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, T> {
        ReaderEx {
            value: self.value.clone(),
            sender: self.sender.clone(),
            func: Arc::new(func),
        }
    }

    /// Num of subscriptions.
    pub async fn num_of_subs(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Get current value of state source.
    pub async fn value(&self) -> S {
        (*self.value.read().await).clone()
    }

    async fn change_ex(
        &self,
        wait_to_end: bool,
        change: Change<S>,
    ) -> Result<(), SourceChangeError> {
        let mut guard = self.value.write().await;
        let (s, not_check_eq) = match change {
            Change::Value(v) => (v, false),
            Change::Func(func) => (func((*guard).clone()), false),
            Change::Touch => ((*guard).clone(), true),
        };
        if not_check_eq || *guard != s {
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
            *guard = s;
            Ok(())
        } else {
            Err(SourceChangeError::NotChange)
        }
    }

    /// Change state of source.
    pub async fn change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Value(s)).await
    }

    /// Change state of source, and wait responders to finish actions upon the change event.
    pub async fn wait_change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Value(s)).await
    }

    /// Change state of source by modifying it with a func.
    pub async fn modify(
        &self,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Func(Arc::new(func))).await
    }

    /// Change state of source by modifying it with a func, and wait responders to finish actions upon the change event.
    pub async fn wait_modify(
        &self,
        func: impl Fn(S) -> S + Send + Sync + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Func(Arc::new(func))).await
    }

    /// Create a change event without changing state of source really.
    pub async fn touch(&self) -> Result<(), SourceChangeError> {
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
    #[error("State source not change, no change detected")]
    NotChange,
}

/// Data structure to be exposed to do subscription by state change responders.
#[derive(Clone)]
pub struct Reader<S> {
    value: Arc<RwLock<S>>,
    sender: broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
}

impl<S> Into<ReaderEx<S, S>> for Reader<S>
where
    S: 'static + Send,
{
    fn into(self) -> ReaderEx<S, S> {
        ReaderEx {
            value: self.value,
            sender: self.sender,
            func: Arc::new(|s| Box::pin(async move { s })),
        }
    }
}

impl<S> Reader<S> {
    pub fn extend<T>(
        &self,
        func: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
    ) -> ReaderEx<S, T> {
        ReaderEx {
            value: self.value.clone(),
            sender: self.sender.clone(),
            func: Arc::new(func),
        }
    }
}

/// Data structure to be exposed to do subscription by state change responders, with the ability to convert the state to another type.
#[derive(Clone)]
pub struct ReaderEx<S, T> {
    value: Arc<RwLock<S>>,
    sender: broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
    func: Arc<dyn Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync>,
}

impl<S, T> ReaderEx<S, T>
where
    S: Clone,
{
    async fn value(&self) -> T {
        self.func.as_ref()((*self.value.read().await).clone()).await
    }
}

/// Data structure to store the latest state in responder's state machine, can be used to do unsubscription.
#[derive(Clone, Debug)]
pub struct Handle<T> {
    cancel_token: CancellationToken,
    value: Arc<RwLock<T>>,
}

impl<T> Handle<T>
where
    T: Clone + PartialEq,
{
    fn new(init_value: T) -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            value: Arc::new(RwLock::new(init_value)),
        }
    }

    async fn store(&self, t: T, not_check_eq: bool) -> bool {
        let changed = *self.value.read().await != t;
        if changed {
            *self.value.write().await = t;
        }
        not_check_eq || changed
    }

    async fn value(&self) -> T {
        (*self.value.read().await).clone()
    }

    /// Unsubscribe operation, this is optional, after your state machine
    /// is dropped, subscriptions are auto cleaned.
    pub fn unsubscribe(&self) {
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
    ) -> Result<(), impl std::error::Error>;
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
    /// - stage [2] -- convert to target type and send to mpsc channel.
    /// - stage [3] -- receive from mpsc channel and process it.
    /// - stage [4] -- (optional) feedback when the change event has been processed.
    #[instrument(name = "UseStateHandle::subscribe", skip_all, fields(tag))]
    async fn subscribe<S>(
        self: Arc<Self>,
        reader: impl Into<ReaderEx<S, T>> + Send,
        tag: G,
    ) -> Handle<T>
    where
        S: 'static + Clone + Debug + PartialEq + Send + Sync,
    {
        let reader_ex = reader.into();
        let handle: Handle<T> = Handle::new(reader_ex.value().await);
        self.state_machine()
            .await
            .add_handle(tag.clone(), handle.clone());
        let mut rx_s = reader_ex.sender.subscribe();
        let (tx_t, mut rx_t) =
            mpsc::unbounded_channel::<(T, T, Option<mpsc::UnboundedSender<()>>)>();
        let handle_c = handle.clone();
        tokio::spawn(async move {
            tracing::info!("Subscription start -- {:?}", tag);
            loop {
                select! {
                    _ = handle_c.cancel_token.cancelled() => {
                        break;
                    }
                    res = rx_s.recv() => {
                        match res {
                            Ok((s, not_check_eq, opt_feedback)) => {
                                let t = reader_ex.func.as_ref()(s).await;
                                let t_old = handle_c.value().await;
                                if handle_c.store(t.clone(), not_check_eq).await {
                                    if let Err(e) = tx_t.send((t, t_old, opt_feedback)) {
                                        tracing::error!("stage [2] | change event send error -- {}", e);
                                        break;
                                    }
                                }
                            },
                            Err(e) => match e {
                                broadcast::error::RecvError::Closed => {
                                    _ = self.state_machine().await.del_source(tag.clone());
                                    tracing::info!("state source channel closed");
                                    break;
                                },
                                broadcast::error::RecvError::Lagged(_) => {
                                    tracing::error!("stage [1] | change event recv lagged");
                                    break;
                                },
                            },
                        }
                    }
                    res = rx_t.recv() => {
                        match res {
                            Some((t, t_old, opt_feedback)) => {
                                let _lock = self.lock().await;
                                if let Err(e) = self.clone().on_change(tag.clone(), t, t_old).await {
                                    tracing::error!("stage [3] | change event proc error -- {}", e);
                                }
                                if let Some(feedback) = opt_feedback && let Err(e) = feedback.send(()) {
                                    tracing::error!("stage [4] | change event feedback error -- {}", e);
                                }
                            },
                            None => {
                                tracing::info!("state target channel closed");
                                break;
                            },
                        }
                    }
                }
            }
            _ = self.state_machine().await.del_handle(tag.clone());
            tracing::info!("Subscription end -- {:?}", tag);
        });
        handle
    }
}

impl<V, T, G> UseStateHandle<T, G> for V
where
    V: 'static + HasStateHandle<T, G>,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}
