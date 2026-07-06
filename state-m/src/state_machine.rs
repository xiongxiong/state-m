use crate::{handle::Handle, source::Source};
use dashmap::DashMap;
use std::{
    any::{Any, type_name},
    cmp::Eq,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct StateMachine<K>(Arc<DashMap<K, Box<dyn Any + Send + Sync>>>)
where
    K: Clone + Debug + Eq + Hash;

impl<K> Deref for StateMachine<K>
where
    K: Clone + Debug + Eq + Hash,
{
    type Target = Arc<DashMap<K, Box<dyn Any + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> StateMachine<K>
where
    K: Clone + Debug + Eq + Hash,
{
    // fn handle(&self) -> Handle<S> {
    //     //
    // }

    /// Add state source into state machine.
    fn add_source<S>(&self, tag: K)
    where
        S: 'static + Clone + Debug + Default + PartialEq,
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
