mod handle;
mod reader;
mod source;
mod state;
mod state_machine;

pub use state::*;
pub use state_m_macro::*;
pub use state_machine::*;
use std::{fmt::Debug, hash::Hash};

pub trait KvAssoc {
    type Value;
}

pub trait AsTag: Clone + Debug + Eq + Hash + Send + Sync {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash + Send + Sync {}
