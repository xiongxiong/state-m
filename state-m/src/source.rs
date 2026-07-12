use crate::{reader::Reader, state::StateEvent};
use std::{fmt::Debug, sync::OnceLock};
use tokio::sync::{
    Mutex,
    broadcast::{Receiver, Sender, channel},
};

pub trait AsSourceState: Clone + Debug + Default + PartialEq {}

impl<T> AsSourceState for T where T: Clone + Debug + Default + PartialEq {}

#[derive(Debug)]
pub(crate) struct Source<S>
where
    S: 'static + AsSourceState,
{
    pub capacity: usize,
    pub sender: Sender<StateEvent<S>>,
    pub recver: Mutex<OnceLock<Receiver<StateEvent<S>>>>,
}

impl<S> Source<S>
where
    S: 'static + AsSourceState,
{
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = channel(capacity);
        let once = OnceLock::new();
        once.set(rx).expect("should not happen");
        Self {
            capacity,
            sender: tx,
            recver: Mutex::new(once),
        }
    }

    pub fn reader(&self) -> Reader<S> {
        let once = OnceLock::new();
        once.set(self.sender.subscribe())
            .expect("should not happen");
        Reader {
            capacity: self.capacity,
            sender: self.sender.clone(),
            recver: Mutex::new(once),
        }
    }
}
