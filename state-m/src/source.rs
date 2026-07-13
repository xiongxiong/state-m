use crate::{reader::Reader, state::StateEvent};
use std::fmt::Debug;
use tokio::sync::broadcast::{Sender, channel};

pub trait AsSourceState: Clone + Debug + Default + PartialEq {}

impl<T> AsSourceState for T where T: Clone + Debug + Default + PartialEq {}

#[derive(Debug)]
pub(crate) struct Source<S>
where
    S: 'static + AsSourceState,
{
    pub capacity: usize,
    pub sender: Sender<StateEvent<S>>,
}

impl<S> Source<S>
where
    S: 'static + AsSourceState,
{
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = channel(capacity);
        Self {
            capacity,
            sender: tx,
        }
    }

    pub fn reader(&self) -> Reader<S> {
        Reader {
            capacity: self.capacity,
            sender: self.sender.clone(),
        }
    }
}
