use crate::{
    handle::Handle,
    source::{AsSourceState, Source},
};
use dashmap::DashMap;
use std::{any::Any, cmp::Eq, fmt::Debug, hash::Hash, ops::Deref, sync::Arc};
use thiserror::Error;
use tokio::select;

pub trait KVAssoc {
    type Value;
}

pub trait AsTag: Clone + Debug + Eq + Hash + Send + Sync {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash + Send + Sync {}

#[derive(Clone, Debug)]
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
}

impl<K> StateMachine<K>
where
    K: AsTag,
{
    /// Add state source into state machine.
    pub fn add_source<S, T>(&self, tag: T) -> Result<(), AddSourceError<T>>
    where
        S: 'static + AsSourceState,
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddSourceError::AlreadyExist(tag));
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
    pub fn del_source<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.remove(&tag.clone().into()).is_some()
    }

    /// If state source of tag exists in state machine.
    pub fn has_source<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.contains_key(&tag.clone().into())
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    pub async fn subscribe<T>(self: Arc<Self>, tag: &T) -> Result<(), SubscribeError<T>>
    where
        Self: UseState<T, K>,
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState + Send + Sync,
    {
        let handle = self.handle(tag)?;
        let cache = handle.cache();
        let recver = handle.recver();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    res = recver.recv() => {
                        match res {
                            Ok(s) => {
                                let s_old = { cache.read().unwrap().clone() };
                                if s.is_touch || s.state.value != s_old.value {
                                    tracing::trace!("StateM | recv -- {:?}", s);
                                    { *cache.write().unwrap() = s.state.clone(); }
                                    this.on_change(s.state.value, s_old.value);
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
}

pub trait UseState<T, K>
where
    T: Clone + Debug + Into<K> + KVAssoc,
    T::Value: 'static + AsSourceState + Send + Sync,
    K: AsTag,
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
pub enum AddSourceError<T>
where
    T: Debug,
{
    #[error("State handle for tag [{0:?}] already exist.")]
    AlreadyExist(T),
}
