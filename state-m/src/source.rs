use crate::{AsState, barrier::AsPassCheck, reader::Reader, state::StateEvent};
use std::{fmt::Debug, ops::Deref, sync::Arc};
use tokio::sync::broadcast::{Sender, channel};

#[derive(Clone, Debug)]
pub struct Inner<S>
where
    S: AsState,
{
    pub(crate) capacity: usize,
    pub(crate) sender: Sender<StateEvent<S>>,
    pub(crate) pass_checks: Arc<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
}

impl<S> Inner<S>
where
    S: AsState,
{
    pub fn new(
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Self {
        let (tx, _) = channel(capacity);
        Self {
            capacity,
            sender: tx,
            pass_checks: match pass_checks {
                Some(chks) => Arc::new(chks),
                None => Arc::new(Vec::new()),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct Source<S>(Inner<S>)
where
    S: AsState;

impl<S> Deref for Source<S>
where
    S: AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> Source<S>
where
    S: AsState,
{
    pub fn new(
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Self {
        Self(Inner::new(capacity, pass_checks))
    }

    pub fn reader(&self) -> Reader<S> {
        Reader::from(self.0.clone())
    }
}
