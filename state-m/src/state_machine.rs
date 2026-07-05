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
pub struct StateMachine<G>(Arc<DashMap<G, Box<dyn Any + Send + Sync>>>)
where
    G: Clone + Debug + Eq + Hash;

impl<G> Deref for StateMachine<G>
where
    G: Clone + Debug + Eq + Hash,
{
    type Target = Arc<DashMap<G, Box<dyn Any + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<G> StateMachine<G>
where
    G: Clone + Debug + Eq + Hash,
{
    fn add_source<S>(&self, tag: G)
    where
        S: 'static + Clone + Debug + Default + PartialEq,
    {
        assert!(
            !self.contains_key(&tag),
            "Source already exist, tag -- {:?}, type -- {:?}",
            tag,
            type_name::<S>()
        );
        self.insert(tag, Box::new(""));
    }
}
