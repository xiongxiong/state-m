mod barrier;
mod handle;
mod reader;
mod source;
mod state;
mod state_machine;

pub use barrier::*;
use chrono::{DateTime, Utc};
pub use reader::*;
pub use state::*;
pub use state_m_macro::StateTag;
pub use state_machine::*;
use std::{fmt::Debug, hash::Hash};

/// Associate a Key type and a Value type.
pub trait KvAssoc {
    type Value;
}

/// If hidden the tag in debug_state or not
pub trait IsHidden {
    const HIDDEN: bool;
}

/// Judge if a key associated with a tag or not.
pub trait KeyIsTag<T> {
    fn predicate(&self) -> bool;
}

/// State type needs to implement these traits: Clone, Debug, Default, PartialEq.
pub trait AsState: 'static + Clone + Debug + Default + PartialEq + Send + Sync {}

impl<T> AsState for T where T: 'static + Clone + Debug + Default + PartialEq + Send + Sync {}

/// Tag type needs to implement these traits: Clone, Debug, Eq, Hash, Send, Sync.
pub trait AsKey: 'static + Clone + Debug + Eq + Hash + Ord + Send + Sync {}

impl<T> AsKey for T where T: 'static + Clone + Debug + Eq + Hash + Ord + Send + Sync {}

/// Structured debug state of handle
#[derive(Clone, PartialEq)]
pub struct DebugState {
    pub label: String,
    pub state: String,
    pub timestamp: DateTime<Utc>,
}

impl Debug for DebugState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<20} | {} | {}",
            self.label, self.state, self.timestamp
        )
    }
}
