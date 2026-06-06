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
    sync::{Mutex, RwLock, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

#[derive(Debug)]
pub struct StateMachine<Tag>
where
    Tag: Eq + Hash,
{
    sources: Arc<DashMap<Tag, Box<dyn Any + Send + Sync>>>,
    targets: Arc<DashMap<Tag, Box<dyn Any + Send + Sync>>>,
}

impl<Tag> Default for StateMachine<Tag>
where
    Tag: Eq + Hash,
{
    fn default() -> Self {
        Self {
            sources: Default::default(),
            targets: Default::default(),
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

    pub async fn source<S>(&self, tag: &Tag) -> Source<S>
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

    pub(crate) fn new_target<T>(&self, tag: Tag, target: EventStore<T>)
    where
        T: 'static + Send + Sync,
    {
        assert!(
            !self.targets.contains_key(&tag),
            "duplicate tag for target -- {:?}",
            tag
        );
        self.targets.insert(tag, Box::new(target));
    }

    pub async fn target_value<T>(&self, tag: &Tag) -> Option<T>
    where
        T: 'static + Clone + PartialEq,
    {
        let opt_target_box = self.targets.get(&tag);
        assert!(
            opt_target_box.is_some(),
            "state target does not exist, tag -- {:?}",
            tag
        );
        let target_box = opt_target_box.unwrap();
        let opt_target = target_box.downcast_ref::<EventStore<T>>();
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

#[async_trait]
pub trait HasStateMachine<Tag>
where
    Tag: Eq + Hash,
{
    async fn state_machine(self: Arc<Self>) -> Arc<Mutex<StateMachine<Tag>>>;
}

#[async_trait]
pub trait UseStateSource<Tag>: HasStateMachine<Tag>
where
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
    async fn new_source<S>(self: Arc<Self>, tag: Tag, source: Source<S>)
    where
        S: 'static + Send + Sync,
    {
        (*self.state_machine().await.lock().await).new_source(tag, source);
    }
}

impl<T, Tag> UseStateSource<Tag> for T
where
    T: HasStateMachine<Tag>,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

type NotCheckEq = bool;

#[derive(Clone, Debug)]
pub struct Source<S> {
    value: Arc<RwLock<S>>,
    sender: Arc<broadcast::Sender<(S, NotCheckEq)>>,
}

impl<S> Source<S>
where
    S: Clone + PartialEq,
{
    pub fn reader(&self) -> Reader<S> {
        Reader {
            sender: self.sender.clone(),
        }
    }

    pub async fn value(&self) -> S {
        (*self.value.read().await).clone()
    }

    pub async fn change(&self, s: S) -> Result<(), SourceChangeError> {
        let mut guard = self.value.write().await;
        if *guard != s {
            self.sender
                .send((s.clone(), false))
                .map_err(|_| SourceChangeError::SendErr)?;
            *guard = s;
            Ok(())
        } else {
            Err(SourceChangeError::NotChange)
        }
    }

    pub async fn modify(&self, func: impl Fn(S) -> S) -> Result<(), SourceChangeError> {
        let mut guard = self.value.write().await;
        let s = func((*guard).clone());
        if *guard != s {
            self.sender
                .send((s.clone(), false))
                .map_err(|_| SourceChangeError::SendErr)?;
            *guard = s;
            Ok(())
        } else {
            Err(SourceChangeError::NotChange)
        }
    }

    pub async fn touch(&self) -> Result<(), broadcast::error::SendError<(S, bool)>> {
        let guard = self.value.read().await;
        self.sender.send(((*guard).clone(), false))?;
        Ok(())
    }
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
    sender: Arc<broadcast::Sender<(S, NotCheckEq)>>,
}

#[derive(Clone, Debug)]
pub struct EventStore<T>(pub(crate) Arc<RwLock<Option<T>>>);

impl<T> EventStore<T>
where
    T: Clone + PartialEq,
{
    pub fn new() -> Self {
        EventStore(Arc::new(RwLock::new(None)))
    }

    async fn store(&self, val: T, not_check_eq: bool) -> bool {
        let opt_t = Some(val);
        let res = *self.0.read().await != opt_t;
        if res {
            *self.0.write().await = opt_t;
        }
        not_check_eq || res
    }

    pub async fn value(&self) -> Option<T> {
        (*self.0.read().await).clone()
    }
}

pub struct SubscribeHandle(CancellationToken);

impl SubscribeHandle {
    pub fn unsubscribe(&self) {
        self.0.cancel();
    }
}

#[async_trait]
pub trait UseStateTarget<S, T, Tag>: HasStateTarget<S, T, Tag>
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
    #[instrument(name = "AsTarget::subscribe", skip_all, fields(tag, chan_cap))]
    async fn subscribe(
        self: Arc<Self>,
        reader: Reader<S>,
        tag: Tag,
        chan_cap: usize,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
    ) -> SubscribeHandle {
        let t_store: EventStore<T> = EventStore::new();
        (*self.clone().state_machine().await.lock().await).new_target(tag.clone(), t_store.clone());
        let mut rx_s = reader.sender.subscribe();
        let (tx_t, mut rx_t) = mpsc::channel::<T>(chan_cap);
        let cancel_token = CancellationToken::new();
        let handle = SubscribeHandle(cancel_token.clone());
        tokio::spawn(async move {
            tracing::info!("Subscription start -- {:?}", tag);
            loop {
                select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    res = rx_s.recv() => {
                        match res {
                            Ok((s, not_check_eq)) => {
                                let t = convert(s).await;
                                if t_store.store(t.clone(), not_check_eq).await {
                                    if let Err(e) = tx_t.send(t).await {
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
                            Some(t) => {
                                let state_machine = self.clone().state_machine().await;
                                let _lock = state_machine.lock().await;
                                if let Err(e) = self.clone().on_change(t, t_store.value().await).await {
                                    tracing::error!("stage [3] | change event proc error -- {}", e);
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

impl<V, S, T, Tag> UseStateTarget<S, T, Tag> for V
where
    V: 'static + HasStateTarget<S, T, Tag>,
    S: 'static + Clone + Debug + Send,
    T: 'static + Clone + Debug + PartialEq + Send + Sync,
    Tag: 'static + Clone + Debug + Eq + Hash + Send + Sync,
{
}

#[async_trait]
pub trait HasStateTarget<S, T, Tag>: HasStateMachine<Tag>
where
    Tag: Eq + Hash,
{
    async fn on_change(self: Arc<Self>, new_value: T, old_value: Option<T>) -> anyhow::Result<()>;
}
