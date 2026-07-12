use crate::{reader::Reader, state::StateEvent};
use std::{fmt::Debug, sync::Arc};
use tokio::sync::{
    Mutex,
    broadcast::{Receiver, Sender, channel},
};

pub trait AsSourceState: Clone + Debug + Default + PartialEq + Unpin {}

impl<T> AsSourceState for T where T: Clone + Debug + Default + PartialEq + Unpin {}

#[derive(Clone, Debug)]
pub(crate) struct Source<S>
where
    S: 'static + AsSourceState,
{
    pub(crate) sender: Arc<Sender<StateEvent<S>>>,
    pub(crate) recver: Arc<Mutex<Receiver<StateEvent<S>>>>,
}

impl<S> Source<S>
where
    S: 'static + AsSourceState,
{
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = channel(capacity);
        Self {
            sender: Arc::new(tx),
            recver: Arc::new(Mutex::new(rx)),
        }
    }

    pub fn reader(&self) -> Reader<S> {
        Reader {
            sender: self.sender.clone(),
            recver: Arc::new(Mutex::new(self.sender.subscribe())),
        }
    }
}
