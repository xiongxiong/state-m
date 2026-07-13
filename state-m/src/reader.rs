use crate::state::{State, StateEvent};
use std::fmt::Debug;
use tokio::{
    select,
    sync::broadcast::{Sender, channel, error::RecvError},
};

#[derive(Debug)]
pub struct Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    pub(crate) capacity: usize,
    pub(crate) sender: Sender<StateEvent<S>>,
}

impl<S> Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Send,
{
    pub fn extend<T>(&self, capacity: usize) -> Reader<T>
    where
        T: 'static + Clone + Debug + Default + From<S> + PartialEq + Send,
    {
        self.extend_with(capacity, |s| async move { T::from(s) })
    }

    pub fn extend_with<T, F, Fut>(&self, capacity: usize, f: F) -> Reader<T>
    where
        T: 'static + Clone + Debug + Default + PartialEq + Send,
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send,
    {
        let (tx, _) = channel(capacity);
        let tx_c = tx.clone();
        let mut rx_o = self.sender.subscribe();
        tokio::spawn(async move {
            loop {
                select! {
                    res = rx_o.recv() => {
                        match res {
                            Ok(s) => {
                                tracing::trace!("recv -- {:?}", s.state);
                                let s_new = StateEvent {
                                    state: State {
                                        value: f(s.state.value).await,
                                        timestamp: s.state.timestamp,
                                    },
                                    is_touch: s.is_touch,
                                    close_handle: s.close_handle,
                                };
                                if tx_c.send(s_new).is_err() {
                                    break;
                                }
                            },
                            Err(e) => {
                                match e {
                                    RecvError::Closed => break,
                                    RecvError::Lagged(n) => {
                                        tracing::warn!("lagged | skipped {n} messages.")
                                    },
                                }
                            },
                        }
                    }
                }
            }
        });
        Reader {
            capacity: self.capacity,
            sender: tx,
        }
    }
}
