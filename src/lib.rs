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

/// state machine
#[derive(Clone, Debug)]
pub struct StateMachine<Tag>
where
    Tag: Eq + Hash,
{
    sources: Arc<DashMap<Tag, Box<dyn Any + Send + Sync>>>,
    handles: Arc<DashMap<Tag, Box<dyn Any + Send + Sync>>>,
}

impl<Tag> Default for StateMachine<Tag>
where
    Tag: Eq + Hash,
{
    fn default() -> Self {
        Self {
            sources: Default::default(),
            handles: Default::default(),
        }
    }
}

impl<Tag> StateMachine<Tag>
where
    Tag: Clone + Debug + Eq + Hash,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub(crate) fn new_source<S>(&self, tag: Tag, source: Source<S>)
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

    /// get source from state machine by tag
    pub async fn source<S>(&self, tag: Tag) -> Source<S>
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

    pub(crate) fn new_target<T>(&self, tag: Tag, target: Handle<T>)
    where
        T: 'static + Send + Sync,
    {
        assert!(
            !self.handles.contains_key(&tag),
            "duplicate tag for target -- {:?}",
            tag
        );
        self.handles.insert(tag, Box::new(target));
    }

    /// get current value of target from state machine
    pub async fn target_value<T>(&self, tag: Tag) -> Option<T>
    where
        T: 'static + Clone + PartialEq,
    {
        let opt_target_box = self.handles.get(&tag);
        assert!(
            opt_target_box.is_some(),
            "state target does not exist, tag -- {:?}",
            tag
        );
        let target_box = opt_target_box.unwrap();
        let opt_target = target_box.downcast_ref::<Handle<T>>();
        assert!(
            opt_target.is_some(),
            "state target does not exist, tag -- {:?}, type -- {}",
            tag,
            type_name::<T>()
        );
        let target = opt_target.unwrap();
        target.value().await
    }
}

/// has state machine
#[async_trait]
pub trait HasStateMachine<Tag>
where
    Tag: Eq + Hash,
{
    async fn lock(&self) -> MutexGuard<'_, ()>;

    async fn state_machine(&self) -> StateMachine<Tag>;
}

/// use state machine
#[async_trait]
pub trait UseStateMachine<Tag>: HasStateMachine<Tag>
where
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// get state source
    async fn source<S>(&self, tag: Tag) -> Source<S>
    where
        S: 'static + Clone,
    {
        self.state_machine().await.source(tag).await
    }

    /// get current value of target
    async fn target_value<T>(&self, tag: Tag) -> Option<T>
    where
        T: 'static + Clone + PartialEq + Send + Sync,
    {
        self.state_machine().await.target_value(tag).await
    }
}

#[async_trait]
impl<T, Tag> UseStateMachine<Tag> for T
where
    T: HasStateMachine<Tag>,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

/// use state source
#[async_trait]
pub trait UseStateSource<Tag>: HasStateMachine<Tag>
where
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// add state source to state machine
    async fn new_source<S>(&self, tag: Tag, source: Source<S>)
    where
        S: 'static + Send + Sync,
    {
        self.state_machine().await.new_source(tag, source);
    }
}

impl<T, Tag> UseStateSource<Tag> for T
where
    T: HasStateMachine<Tag>,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

type NotCheckEq = bool;

/// state source
#[derive(Clone, Debug)]
pub struct Source<S> {
    value: Arc<RwLock<Option<S>>>,
    sender: Arc<broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>>,
}

impl<S> Source<S>
where
    S: Clone + PartialEq,
{
    pub fn new() -> Self {
        Self::create(None, 100)
    }

    pub fn create(init_value: Option<S>, capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            value: Arc::new(RwLock::new(init_value)),
            sender: Arc::new(tx),
        }
    }

    /// get reader of state source, can be used to subscribe by state target
    pub fn reader(&self) -> Reader<S> {
        Reader {
            sender: self.sender.clone(),
        }
    }

    /// get current value of state source
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

    /// change state of source
    pub async fn change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Value(s)).await
    }

    /// change state of source, and wait handles to finish actions upon the change event
    pub async fn wait_change(&self, s: S) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Value(s)).await
    }

    /// change state of source by modifying it with a func
    pub async fn modify(&self, func: impl Fn(S) -> S + 'static) -> Result<(), SourceChangeError> {
        self.change_ex(false, Change::Func(Box::new(func))).await
    }

    /// change state of source by modifying it with a func, and wait handles to finish actions upon the change event
    pub async fn wait_modify(
        &self,
        func: impl Fn(S) -> S + 'static,
    ) -> Result<(), SourceChangeError> {
        self.change_ex(true, Change::Func(Box::new(func))).await
    }

    /// create a change event without changing state of source really
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
    #[error("state source broadcast error")]
    SendErr,
    #[error("state source not change, no change detected")]
    NotChange,
}

#[derive(Clone, Debug)]
pub struct Reader<S> {
    sender: Arc<broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>>,
}

/// store the latest state in target
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

    /// unsubscribe
    pub fn unsubscribe(&self) {
        self.cancel_token.cancel();
    }
}

/// define action upon state change
#[async_trait]
pub trait HasStateTarget<S, T, Tag>: HasStateMachine<Tag>
where
    Tag: Eq + Hash,
{
    /// action upon state change
    async fn on_change(
        self: Arc<Self>,
        tag: Tag,
        new_value: T,
        old_value: Option<T>,
    ) -> anyhow::Result<()>;
}

#[async_trait]
pub trait UseStateConvTarget<S, T, Tag>: HasStateTarget<S, T, Tag>
where
    Self: 'static,
    S: 'static + Clone + Debug + Send,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    /// subscribe
    /// stage [1] -- receive from source's broadcast channel
    /// stage [2] -- convert to target type and send to mpsc channel
    /// stage [3] -- receive from mpsc channel and process it
    /// stage [4] -- (optional) feedback when the change event has been processed
    #[instrument(
        name = "UseStateConvTarget::convert_subscribe",
        skip_all,
        fields(tag, chan_cap)
    )]
    async fn convert_subscribe(
        self: Arc<Self>,
        reader: Reader<S>,
        tag: Tag,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
    ) -> Handle<T> {
        let handle: Handle<T> = Handle::new();
        self.state_machine()
            .await
            .new_target(tag.clone(), handle.clone());
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
            tracing::info!("Subscription end -- {:?}", tag);
        });
        handle
    }
}

impl<V, S, T, Tag> UseStateConvTarget<S, T, Tag> for V
where
    V: 'static + HasStateTarget<S, T, Tag>,
    S: 'static + Clone + Debug + Send,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

#[async_trait]
pub trait UseStateTarget<T, Tag>: UseStateConvTarget<T, T, Tag>
where
    Self: 'static,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    #[instrument(name = "UseStateTarget::subscribe", skip_all, fields(tag, chan_cap))]
    async fn subscribe(self: Arc<Self>, reader: Reader<T>, tag: Tag) -> Handle<T> {
        UseStateConvTarget::convert_subscribe(self, reader, tag, |t| Box::pin(async move { t }))
            .await
    }
}

impl<V, T, Tag> UseStateTarget<T, Tag> for V
where
    V: 'static + UseStateConvTarget<T, T, Tag>,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}
