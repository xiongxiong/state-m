use crate::{reader::Reader, state::StateEvent};
use crossfire::{
    MAsyncRx, MAsyncTx,
    mpmc::{self, List},
};
use std::fmt::Debug;

pub trait AsSourceState: Clone + Debug + Default + PartialEq + Unpin {}

impl<T> AsSourceState for T where T: Clone + Debug + Default + PartialEq + Unpin {}

#[derive(Clone, Debug)]
pub(crate) struct Source<S>
where
    S: 'static + AsSourceState,
{
    pub(crate) sender: MAsyncTx<List<StateEvent<S>>>,
    pub(crate) recver: MAsyncRx<List<StateEvent<S>>>,
}

impl<S> Default for Source<S>
where
    S: 'static + AsSourceState,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Source<S>
where
    S: 'static + AsSourceState,
{
    pub fn new() -> Self {
        let (tx, rx) = mpmc::unbounded_async();
        Self {
            sender: tx.into_async(),
            recver: rx,
        }
    }

    pub fn reader(&self) -> Reader<S> {
        Reader {
            recver: self.recver.clone(),
        }
    }
}
