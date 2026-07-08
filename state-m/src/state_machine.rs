use crate::{
    handle::{Handle, StateChangeError as HandleStateChangeError},
    reader::Reader,
    source::{AsSourceState, Source},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{any::Any, cmp::Eq, fmt::Debug, hash::Hash, ops::Deref, sync::Arc};
use thiserror::Error;
use tokio::select;
use tracing::instrument;

pub trait KVAssoc {
    type Value;
}

pub trait AsTag: Clone + Debug + Eq + Hash + Send + Sync {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash + Send + Sync {}

/// StateMachine: data structure to store handles.
/// - K - to distinguish different handles.
#[derive(Clone, Debug, Default)]
pub struct StateMachine<K>(Arc<DashMap<K, Arc<dyn Any + Send + Sync>>>)
where
    K: AsTag;

impl<K> Deref for StateMachine<K>
where
    K: AsTag,
{
    type Target = Arc<DashMap<K, Arc<dyn Any + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> StateMachine<K>
where
    K: AsTag,
{
    fn handle<T>(&self, tag: &T) -> Result<Handle<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: AsSourceState,
    {
        let k = tag.clone().into();
        match self.get(&k) {
            Some(v) => match v.downcast_ref::<Handle<T::Value>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag.clone())),
        }
    }

    /// Add state reader into state machine.
    fn add_reader<T>(&self, tag: T, reader: Reader<T::Value>) -> Result<(), AddHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag));
        }
        self.insert(k, Arc::new(Handle::Reader(reader, Default::default())));
        Ok(())
    }

    #[instrument(level = "trace", skip(self, state_user))]
    async fn subscribe<T>(
        &self,
        tag: &T,
        state_user: Arc<dyn UseState<T> + Send + Sync>,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let handle = self.handle(tag)?;
        let cache = handle.cache();
        let recver = handle.recver();
        tokio::spawn(async move {
            loop {
                select! {
                    res = recver.recv() => {
                        match res {
                            Ok(s) => {
                                let s_old = { cache.read().unwrap().clone() };
                                if s.is_touch || s.state.value != s_old.value {
                                    tracing::debug!("StateM | recv -- {:?}", s);
                                    { *cache.write().unwrap() = s.state.clone(); }
                                    state_user.on_change(s.state.value, s_old.value);
                                }
                            },
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn subscribe_reader<T>(
        &self,
        tag: &T,
        reader: Reader<T::Value>,
        state_user: Arc<dyn UseState<T> + Send + Sync>,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.add_reader(tag.clone(), reader)?;
        self.subscribe(tag, state_user).await
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    /// Add state source into state machine.
    pub fn add_source<T>(&self, tag: T) -> Result<(), AddHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag));
        }
        self.insert(
            k,
            Arc::new(Handle::Source(
                Source::<T::Value>::new(),
                Default::default(),
            )),
        );
        Ok(())
    }

    /// Remove state source from state machine.
    pub fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.remove(&tag.clone().into()).is_some()
    }

    /// If state source of tag exists in state machine.
    pub fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.contains_key(&tag.clone().into())
    }

    pub fn reader<T>(&self, tag: &T) -> Result<Reader<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: AsSourceState,
    {
        Ok(self.handle(tag)?.reader())
    }

    pub fn value<T>(&self, tag: &T) -> Result<T::Value, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.value())
    }

    pub fn value_ex<T>(&self, tag: &T) -> Result<(T::Value, DateTime<Utc>), GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.value_ex())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn touch<T>(&self, tag: &T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.touch().await?)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn wait_touch<T>(&self, tag: &T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_touch().await?)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn alter<T>(&self, tag: &T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.alter(s).await?)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn wait_alter<T>(&self, tag: &T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_alter(s).await?)
    }

    #[instrument(level = "trace", skip(self, f))]
    pub async fn amend<T>(
        &self,
        tag: &T,
        f: impl FnOnce(&T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.amend(f).await?)
    }

    #[instrument(level = "trace", skip(self, f))]
    pub async fn wait_amend<T>(
        &self,
        tag: &T,
        f: impl FnOnce(&T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_amend(f).await?)
    }
}

pub trait HasStateMachine<K>
where
    K: AsTag,
{
    fn state_machine(&self) -> StateMachine<K>;
}

#[async_trait]
pub trait UseStateMachine<K>: HasStateMachine<K>
where
    K: AsTag,
{
    async fn subscribe<T>(self: Arc<Self>, tag: &T) -> Result<(), SubscribeError<T>>
    where
        Self: UseState<T>,
        T: 'static + Clone + Debug + Into<K> + KVAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn subscribe_reader<T>(
        self: Arc<Self>,
        tag: &T,
        reader: Reader<T::Value>,
    ) -> Result<(), SubscribeError<T>>
    where
        Self: UseState<T>,
        T: 'static + Clone + Debug + Into<K> + KVAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;
}

#[async_trait]
impl<K, M> UseStateMachine<K> for M
where
    M: 'static + HasStateMachine<K> + Send + Sync,
    K: AsTag,
{
    async fn subscribe<T>(self: Arc<Self>, tag: &T) -> Result<(), SubscribeError<T>>
    where
        Self: UseState<T>,
        T: 'static + Clone + Debug + Into<K> + KVAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().subscribe(tag, self.clone()).await
    }

    async fn subscribe_reader<T>(
        self: Arc<Self>,
        tag: &T,
        reader: Reader<T::Value>,
    ) -> Result<(), SubscribeError<T>>
    where
        Self: UseState<T>,
        T: 'static + Clone + Debug + Into<K> + KVAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine()
            .subscribe_reader(tag, reader, self.clone())
            .await
    }
}

pub trait UseState<T>
where
    T: Clone + Debug + KVAssoc,
    T::Value: 'static + AsSourceState + Send + Sync,
{
    fn on_change(&self, new: T::Value, old: T::Value);
}

#[derive(Debug, Error)]
pub enum SubscribeError<T>
where
    T: Debug,
{
    #[error(transparent)]
    GetHandleError(#[from] GetHandleError<T>),
    #[error(transparent)]
    AddHandleError(#[from] AddHandleError<T>),
}

#[derive(Debug, Error)]
pub enum StateChangeError<T>
where
    T: Debug + KVAssoc,
    T::Value: Default,
{
    #[error(transparent)]
    GetHandleError(#[from] GetHandleError<T>),
    #[error(transparent)]
    StateChangeError(#[from] HandleStateChangeError<T::Value>),
}

#[derive(Debug, Error)]
pub enum GetHandleError<T>
where
    T: Debug,
{
    #[error("State handle for tag [{0:?}] not exist.")]
    HandleNotExist(T),
    #[error("Type of state value does not match.")]
    TypeNotMatch,
}

#[derive(Debug, Error)]
pub enum AddHandleError<T>
where
    T: Debug,
{
    #[error("State handle for tag [{0:?}] already exist.")]
    AlreadyExist(T),
}
