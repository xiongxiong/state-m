use crate::{
    AsTag, KvAssoc, StateEvent,
    handle::{Handle, StateChangeError as HandleStateChangeError},
    reader::Reader,
    source::{AsSourceState, Source},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{any::Any, fmt::Debug, ops::Deref, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::select;
use tracing::instrument;

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
    fn handle<T>(&self, tag: T) -> Result<Handle<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsSourceState,
    {
        let k = tag.clone().into();
        match self.get(&k) {
            Some(v) => match v.downcast_ref::<Handle<T::Value>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag)),
        }
    }

    /// Add state reader into state machine.
    fn add_reader<T>(&self, tag: T, reader: Reader<T::Value>)
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let k = tag.clone().into();
        if !self.contains_key(&k) {
            self.insert(k, Arc::new(Handle::Reader(reader, Default::default())));
        }
    }

    async fn on_event<T, A, C, F>(
        &self,
        tag: T,
        s: StateEvent<T::Value>,
        conv_s: C,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
        C: Fn(T::Value) -> A,
        F: Fn(A, A, K) -> Pin<Box<dyn Future<Output = anyhow::Result<()>>>>,
    {
        let handle = self.handle(tag.clone())?;
        if let Some((v_new, v_old)) = handle.on_event(s).await
            && let Err(e) = on_change(conv_s(v_new), conv_s(v_old), tag.into()).await
        {
            tracing::error!("StateM | on_change error -- {:?}", e);
        }
        Ok(())
    }

    #[instrument(level = "trace", skip(self, on_change))]
    async fn subscribe<T, F>(&self, tag: T, on_change: F) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        let handle = self.handle(tag)?;
        let recver = handle.recver();
        tokio::spawn(async move {
            loop {
                select! {
                    res = recver.recv() => {
                        match res {
                            Ok(s) => {
                                if let Some((v_new, v_old)) = handle.on_event(s).await && let Err(e) = on_change(v_new, v_old).await {
                                    tracing::error!("StateM | on_change error -- {:?}", e);
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

    async fn subscribe_reader<T, F>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.add_reader(tag.clone(), reader);
        self.subscribe(tag, on_change).await
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    /// Add state source into state machine.
    fn add_source<T>(&self, tag: T) -> Result<(), AddHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
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
    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.remove(&tag.clone().into()).is_some()
    }

    /// If state source of tag exists in state machine.
    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.contains_key(&tag.clone().into())
    }

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsSourceState,
    {
        Ok(self.handle(tag)?.reader())
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.value().await)
    }

    async fn value_ex<T>(&self, tag: T) -> Result<(T::Value, DateTime<Utc>), GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.value_ex().await)
    }

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.touch().await?)
    }

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_touch().await?)
    }

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.alter(s).await?)
    }

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_alter(s).await?)
    }

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.amend(f).await?)
    }

    pub async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsSourceState,
    {
        Ok(self.handle(tag)?.wait_amend(f).await?)
    }
}

pub trait HasStateMachine {
    type K: AsTag;

    fn state_machine(&self) -> StateMachine<Self::K>;
}

#[async_trait]
pub trait UseStateMachine: HasStateMachine {
    /// Add state source into state machine.
    async fn add_source<T, F>(
        self: Arc<Self>,
        tag: T,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send;

    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>;

    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>;

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsSourceState;

    async fn subscribe<T, F>(
        self: Arc<Self>,
        tag: T,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send;

    async fn subscribe_reader<T, F>(
        self: Arc<Self>,
        tag: T,
        reader: Reader<T::Value>,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send;

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn value_ex<T>(&self, tag: T) -> Result<(T::Value, DateTime<Utc>), GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync;
}

#[async_trait]
impl<M> UseStateMachine for M
where
    M: 'static + HasStateMachine + Send + Sync,
{
    async fn add_source<T, F>(
        self: Arc<Self>,
        tag: T,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.state_machine().add_source(tag.clone())?;
        self.subscribe(tag, on_change).await?;
        Ok(())
    }

    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>,
    {
        self.state_machine().del_handle(tag)
    }

    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>,
    {
        self.state_machine().has_handle(tag)
    }

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsSourceState,
    {
        self.state_machine().reader(tag)
    }

    async fn subscribe<T, F>(self: Arc<Self>, tag: T, on_change: F) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.state_machine().subscribe(tag, on_change).await
    }

    async fn subscribe_reader<T, F>(
        self: Arc<Self>,
        tag: T,
        reader: Reader<T::Value>,
        on_change: F,
    ) -> Result<(), SubscribeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
        F: 'static
            + Fn(T::Value, T::Value) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.state_machine()
            .subscribe_reader(tag, reader, on_change)
            .await
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().value(tag).await
    }

    async fn value_ex<T>(&self, tag: T) -> Result<(T::Value, DateTime<Utc>), GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().value_ex(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().wait_touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().alter(tag, s).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().wait_alter(tag, s).await
    }

    #[instrument(level = "trace", skip(self, f))]
    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().amend(tag, f).await
    }

    #[instrument(level = "trace", skip(self, f))]
    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        self.state_machine().wait_amend(tag, f).await
    }
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
    T: Debug + KvAssoc,
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
