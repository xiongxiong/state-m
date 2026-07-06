use crate::{
    handle::Handle,
    source::{AsSourceState, Source},
};
use dashmap::DashMap;
use std::{any::Any, cmp::Eq, fmt::Debug, hash::Hash, ops::Deref, sync::Arc};
use thiserror::Error;

pub trait KVAssoc {
    type Value;
}

pub trait AsTag: Clone + Debug + Eq + Hash {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash {}

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

    pub async fn subscribe<T>(&self, tag: &T)
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: 'static + AsSourceState,
    {
        let handler = self.handle(tag);
    }
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
