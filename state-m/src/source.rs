use crate::{AsState, reader::Reader, state::StateEvent};
use std::{fmt::Debug, ops::Deref};
use tokio::sync::broadcast::{Sender, channel};

#[derive(Clone, Debug)]
pub struct Inner<S>
where
    S: 'static + AsState,
{
    pub(crate) capacity: usize,
    pub(crate) sender: Sender<StateEvent<S>>,
}

impl<S> Inner<S>
where
    S: 'static + AsState,
{
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = channel(capacity);
        Self {
            capacity,
            sender: tx,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Source<S>(Inner<S>)
where
    S: 'static + AsState;

impl<S> Deref for Source<S>
where
    S: 'static + AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> Source<S>
where
    S: 'static + AsState,
{
    pub fn new(capacity: usize) -> Self {
        Self(Inner::new(capacity))
    }

    pub fn reader(&self) -> Reader<S> {
        Reader::new(self.capacity, self.sender.clone())
    }
}
