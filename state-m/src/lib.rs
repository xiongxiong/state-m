mod handle;
mod reader;
mod source;
mod state;
mod state_machine;

pub use state::*;
pub use state_machine::*;
use std::{fmt::Debug, hash::Hash};

pub trait KVAssoc {
    type Value;
}

pub trait AsTag: Clone + Debug + Eq + Hash + Send + Sync {}

impl<T> AsTag for T where T: Clone + Debug + Eq + Hash + Send + Sync {}
