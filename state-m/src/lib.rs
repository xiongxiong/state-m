mod barrier;
mod handle;
mod reader;
mod source;
mod state;
mod state_machine;

pub use barrier::*;
pub use reader::Reader;
pub use state::*;
pub use state_m_macro::StateTag;
pub use state_machine::*;
use std::{fmt::Debug, hash::Hash};

/// Associate a Key type and a Value type.
pub trait KvAssoc {
    type Key;
    type Value;
}

/// State type needs to implement these traits: Clone, Debug, Default, PartialEq.
pub trait AsState: Clone + Debug + Default + PartialEq {}

impl<T> AsState for T where T: Clone + Debug + Default + PartialEq {}

/// Tag type needs to implement these traits: Clone, Debug, Eq, Hash, Send, Sync.
pub trait AsKey: Clone + Debug + Eq + Hash + Ord + Send + Sync {}

impl<T> AsKey for T where T: Clone + Debug + Eq + Hash + Ord + Send + Sync {}
