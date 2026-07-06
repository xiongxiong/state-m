use crate::{
    core::KVAssoc,
    handle::Handle,
    source::{AsSourceState, Source},
};
use dashmap::DashMap;
use std::{
    any::{Any, type_name},
    cmp::Eq,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
};
use thiserror::Error;

pub trait AsTag: Clone + Debug + Eq + Hash {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash {}

#[derive(Clone, Debug)]
pub struct StateMachine<K>(Arc<DashMap<K, Box<dyn Any + Send + Sync>>>)
where
    K: AsTag;

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
    K: AsTag,
{
    fn handle<T>(&self, tag: T) -> Result<&Handle<T::Value>, StateMachineError<T>>
    where
        T: Clone + Debug + Into<K> + KVAssoc,
        T::Value: AsSourceState,
    {
        let k = tag.clone().into();
        let box_handle = self.get(&k);
        if box_handle.is_none() {
            return Err(StateMachineError::HandleNotExist(tag));
        }
        let opt_handle = box_handle.unwrap().downcast_ref::<Handle<T::Value>>();
        if opt_handle.is_none() {
            return Err(StateMachineError::TypeNotMatch);
        }
        Ok(opt_handle.unwrap())
    }

    /// Add state source into state machine.
    fn add_source<S>(&self, tag: K)
    where
        S: 'static + AsSourceState,
    {
        assert!(
            !self.contains_key(&tag),
            "State source for tag [{:?} | {:?}] already exist, please use a different tag.",
            tag,
            type_name::<S>()
        );
        self.insert(tag, Box::new(""));
    }

    /// Remove state source from state machine.
    fn del_source(&self, tag: &K) -> bool {
        self.remove(tag).is_some()
    }

    /// If state source of tag exists in state machine.
    fn has_source(&self, tag: &K) -> bool {
        self.contains_key(tag)
    }
}

#[derive(Debug, Error)]
pub enum StateMachineError<T>
where
    T: Debug,
{
    #[error("State handle for tag [{0:?}] not exist.")]
    HandleNotExist(T),
    #[error("Type of state value does not match.")]
    TypeNotMatch,
}
