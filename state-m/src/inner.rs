use crate::{AsState, barrier::AsPassCheck, state::StateEvent};
use std::{fmt::Debug, sync::Arc};
use tokio::sync::broadcast::{Sender, channel};

#[derive(Clone, Debug)]
pub struct Inner<S>
where
    S: AsState,
{
    pub(crate) capacity: usize,
    pub(crate) sender: Sender<StateEvent<S>>,
    pub(crate) pass_checks: Arc<Vec<Arc<dyn AsPassCheck + Send + Sync>>>,
}

impl<S> Inner<S>
where
    S: AsState,
{
    pub fn new(
        capacity: usize,
        pass_checks: Option<Vec<Arc<dyn AsPassCheck + Send + Sync>>>,
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
