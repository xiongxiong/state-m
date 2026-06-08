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
    pub async fn source_value<S>(&self, tag: G) -> Option<S>
    where
        S: 'static + Clone + PartialEq,
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
    pub async fn handle_value<T>(&self, tag: G) -> Option<T>
    where
        T: 'static + Clone + PartialEq,
    {
        self.handle(tag).await.value().await
    }
}

/// The trait defined basic methods to use state machine, usually you need a 'Mutex<()>' and a 'StateMachine<G>' in your data structure.
#[async_trait]
pub trait HasStateMachine<G>
where
    G: Clone + Debug + Eq + Hash,
{
    /// The mutex lock to use when responding state change.
    async fn lock(&self) -> MutexGuard<'_, ()>;

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
    async fn handle_value<T>(&self, tag: G) -> Option<T>
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
    /// Add state source to state machine.
    async fn add_source<S>(&self, tag: G, source: Source<S>)
    where
        S: 'static + Send + Sync,
    {
        self.state_machine().await.add_source(tag, source);
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
    value: Arc<RwLock<Option<S>>>,
    sender: Arc<broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>>,
}

impl<S> Source<S>
where
    S: Clone + PartialEq,
{
    /// Create a state source, with broadcast channel capacity of 100.
    pub fn new() -> Self {
        Self::create(100)
    }

    /// Create a state source with custom broadcast channel capacity.
    /// - capacity: broadcast channel capacity
    pub fn create(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            value: Arc::new(RwLock::new(None)),
            sender: Arc::new(tx),
        }
    }

    /// Get reader of state source, can be subscribed by responders.
    pub fn reader(&self) -> Reader<S> {
        Reader {
            sender: self.sender.clone(),
        }
    }

    /// Num of subscriptions.
    pub async fn num_of_subs(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Get current value of state source.
    pub async fn value(&self) -> Option<S> {
        (*self.value.read().await).clone()
    }

    async fn change_ex(
        &self,
        wait_to_end: bool,
        change: Change<S>,
    ) -> Result<(), SourceChangeError> {
        let mut guard = self.value.write().await;
        let (opt_s, not_check_eq) = match change {
            Change::Value(v) => (Some(v), false),
            Change::Func(func) => ((*guard).clone().map(|v| func(v)), false),
            Change::Touch => ((*guard).clone(), true),
        };
        if not_check_eq || *guard != opt_s {
            if let Some(s) = opt_s {
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
                *guard = Some(s);
            }
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
    pub async fn modify(&self, func: impl Fn(S) -> S + 'static) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Func(Box::new(func))).await
    }

    /// Change state of source by modifying it with a func, and wait responders to finish actions upon the change event.
    pub async fn wait_modify(
        &self,
        func: impl Fn(S) -> S + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Func(Box::new(func))).await
    }

    /// Create a change event without changing state of source really.
    pub async fn touch(&self) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Touch).await
    }
}

enum Change<S> {
    Value(S),
    Func(Box<dyn Fn(S) -> S>),
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
#[derive(Clone, Debug)]
pub struct Reader<S> {
    sender: Arc<broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>>,
}

/// Data structure to store the latest state in responder's state machine, can be used to do unsubscription.
#[derive(Clone, Debug)]
pub struct Handle<T> {
    cancel_token: CancellationToken,
    value: Arc<RwLock<Option<T>>>,
}

impl<T> Handle<T>
where
    T: Clone + PartialEq,
{
    fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            value: Arc::new(RwLock::new(None)),
        }
    }

    async fn store(&self, val: T, not_check_eq: bool) -> bool {
        let opt_t = Some(val);
        let res = *self.value.read().await != opt_t;
        if res {
            *self.value.write().await = opt_t;
        }
        not_check_eq || res
    }

    async fn value(&self) -> Option<T> {
        (*self.value.read().await).clone()
    }

    /// Unsubscribe operation, this is optional, after your state machine
    /// is dropped, subscriptions are auto cleaned.
    pub fn unsubscribe(&self) {
        self.cancel_token.cancel();
    }
}

/// Define action upon state change event.
/// - S - type of state in source,
/// - T - type of state in handle,
/// - G - to distinguish different initiators or responders,
/// all initiators must use different tag values, all responders,
/// and all responders do the same, a same tag value can be used
/// by an initiator and a responder in the same state machine.
#[async_trait]
pub trait HasStateHandle<S, T, G>: HasStateMachine<G>
where
    S: Clone + Debug + PartialEq,
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
        old_value: Option<T>,
    ) -> anyhow::Result<()>;
}

/// Convenient method to do subscription with a state convert function. The trait is auto implemented for types implemented HasStateHandle.
#[async_trait]
pub trait UseStateConvHandle<S, T, G>: HasStateHandle<S, T, G>
where
    Self: 'static,
    S: 'static + Clone + Debug + PartialEq + Send,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Do subscription with a state convert function.
    /// - stage [1] -- receive from source's broadcast channel.
    /// - stage [2] -- convert to target type and send to mpsc channel.
    /// - stage [3] -- receive from mpsc channel and process it.
    /// - stage [4] -- (optional) feedback when the change event has been processed.
    #[instrument(
        name = "UseStateConvTarget::convert_subscribe",
        skip_all,
        fields(tag, chan_cap)
    )]
    async fn convert_subscribe(
        self: Arc<Self>,
        reader: Reader<S>,
        tag: G,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
    ) -> Handle<T> {
        let handle: Handle<T> = Handle::new();
        self.state_machine()
            .await
            .add_handle(tag.clone(), handle.clone());
        let mut rx_s = reader.sender.subscribe();
        let (tx_t, mut rx_t) =
            mpsc::unbounded_channel::<(T, Option<T>, Option<mpsc::UnboundedSender<()>>)>();
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
                                let t = convert(s).await;
                                let opt_t_old = handle_c.value().await;
                                if handle_c.store(t.clone(), not_check_eq).await {
                                    if let Err(e) = tx_t.send((t, opt_t_old, opt_feedback)) {
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
                            Some((t, opt_t_old, opt_feedback)) => {
                                let _lock = self.lock().await;
                                if let Err(e) = self.clone().on_change(tag.clone(), t, opt_t_old).await {
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

impl<V, S, T, G> UseStateConvHandle<S, T, G> for V
where
    V: 'static + HasStateHandle<S, T, G>,
    S: 'static + Clone + Debug + PartialEq + Send,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

/// Convenient method to do subscription. The trait is auto implemented for types implemented HasStateHandle.
#[async_trait]
pub trait UseStateHandle<T, G>: UseStateConvHandle<T, T, G>
where
    Self: 'static,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// Do subscription.
    #[instrument(name = "UseStateTarget::subscribe", skip_all, fields(tag, chan_cap))]
    async fn subscribe(self: Arc<Self>, reader: Reader<T>, tag: G) -> Handle<T> {
        UseStateConvHandle::convert_subscribe(self, reader, tag, |t| Box::pin(async move { t }))
            .await
    }
}

impl<V, T, G> UseStateHandle<T, G> for V
where
    V: 'static + UseStateConvHandle<T, T, G>,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    G: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}
