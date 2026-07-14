use crate::{
    AsState, AsTag, KvAssoc, State,
    handle::{Handle, StateChangeError as HandleStateChangeError},
    reader::Reader,
    source::Source,
};
use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use state_m_macro::sm_join;
use std::{any::Any, fmt::Debug, ops::Deref, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::select;
use tracing::instrument;

/// StateMachine: data structure to store handles.
/// - K - to distinguish different handles.
#[derive(Clone, Debug)]
pub struct StateMachine<K>(Arc<DashMap<K, Box<dyn Any + Send + Sync>>>)
where
    K: AsTag;

impl<K> Default for StateMachine<K>
where
    K: AsTag,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K> Deref for StateMachine<K>
where
    K: AsTag,
{
    type Target = Arc<DashMap<K, Box<dyn Any + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    fn get_handle<T>(&self, tag: T) -> Result<Arc<Handle<T::Value>>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsState,
    {
        let k = tag.clone().into();
        match self.get(&k) {
            Some(v) => match v.downcast_ref::<Arc<Handle<T::Value>>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag)),
        }
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    /// Add state source into state machine.
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag));
        }
        let h = Arc::new(Handle::from_source(Source::<T::Value>::new(capacity)));
        h.init(tag).await;
        self.insert(k, Box::new(h));
        Ok(())
    }

    /// Add state reader into state machine.
    async fn add_reader<T>(&self, tag: T, reader: Reader<T::Value>) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag));
        }
        let h = Arc::new(Handle::from_reader(reader));
        h.init(tag).await;
        self.insert(k, Box::new(h));
        Ok(())
    }

    /// Remove state source from state machine.
    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsState,
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
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.reader())
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.value().await)
    }

    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.state().await)
    }

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.touch().await?)
    }

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_touch().await?)
    }

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.alter(s).await?)
    }

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_alter(s).await?)
    }

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.amend(f).await?)
    }

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_amend(f).await?)
    }
}

pub enum StateChange<T>
where
    T: KvAssoc,
    T::Value: AsState,
{
    Change(State<T::Value>, State<T::Value>),
    UnChange(State<T::Value>),
}

impl<T> StateChange<T>
where
    T: KvAssoc,
    T::Value: AsState,
{
    pub fn cur(&self) -> State<T::Value> {
        match self {
            StateChange::Change(v, _) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }

    pub fn old(&self) -> State<T::Value> {
        match self {
            StateChange::Change(_, v) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    // sm_join!(1);
    // sm_join!(2);
    // sm_join!(3);
    // sm_join!(4);
    // sm_join!(5);
    // sm_join!(6);
    // sm_join!(7);
    // sm_join!(8);
    // sm_join!(9);
}

pub trait HasStateMachine {
    type K: AsTag;

    fn state_machine(&self) -> &StateMachine<Self::K>;
}

#[async_trait]
pub trait UseStateMachine: HasStateMachine {
    /// Add state source into state machine.
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsState;

    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>;

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<T>>
    where
        T: Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsState;

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    // pub async fn join_1<T1, F>(&self, tag_1: T1, func: F) -> anyhow::Result<()>
    //     where
    //         T1: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
    //         T1::Value: 'static + AsState + Send,
    //         F: 'static
    //             + Fn(
    //                 Option<(State<T1::Value>, State<T1::Value>)>,
    //             ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send;
}

#[async_trait]
impl<M> UseStateMachine for M
where
    M: 'static + HasStateMachine + Send + Sync,
{
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine()
            .add_source(tag.clone(), capacity)
            .await?;
        Ok(())
    }

    fn del_handle<T>(&self, tag: &T) -> bool
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsState,
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
        T::Value: AsState,
    {
        self.state_machine().reader(tag)
    }

    async fn add_reader<T>(&self, tag: T, reader: Reader<T::Value>) -> Result<(), AddHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().add_reader(tag, reader).await?;
        Ok(())
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().value(tag).await
    }

    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().state(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().alter(tag, s).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T::Value: 'static + AsState + Send + Sync,
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
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_amend(tag, f).await
    }
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
