use crate::{
    AsState,
    source::Inner,
    state::{State, StateEvent},
};
use std::{fmt::Debug, ops::Deref};
use tokio::{
    select,
    sync::broadcast::{Sender, channel, error::RecvError},
};

/// Reader of state, to receive state change events.
#[derive(Clone, Debug)]
pub struct Reader<S>(Inner<S>)
where
    S: 'static + AsState;

impl<S> Deref for Reader<S>
where
    S: 'static + AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> Reader<S>
where
    S: 'static + AsState,
{
    pub(crate) fn from(inner: Inner<S>) -> Self {
        Self(inner)
    }

    pub(crate) fn new(capacity: usize, sender: Sender<StateEvent<S>>) -> Self {
        Self(Inner {
            capacity,
            sender,
            pass_checks: Default::default(),
        })
    }
}

impl<S> Reader<S>
where
    S: 'static + AsState + Send,
{
    /// Check is the channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.sender.subscribe().is_closed()
    }

    /// Convert data type of state reader.
    /// # Arguments
    /// * `capacity` - capacity of the new broadcast channel will be created.
    /// # Returns
    /// Reader of new data type.
    pub fn extend<T>(&self, capacity: usize) -> Reader<T>
    where
        T: AsState + From<S> + Send,
    {
        self.extend_with(capacity, T::from)
    }

    /// Convert data type of state reader, with an closure.
    /// # Arguments
    /// * `capacity` - capacity of the new broadcast channel will be created.
    /// * `f` - an closure, which takes the old state value as parameter, and return the new state value.
    /// # Returns
    /// Reader of new data type.
    pub fn extend_with<T, F>(&self, capacity: usize, f: F) -> Reader<T>
    where
        T: AsState + Send,
        F: Fn(S) -> T + Send + 'static,
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
                                        value: f(s.state.value),
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
        Reader(Inner {
            capacity: self.capacity,
            sender: tx,
            pass_checks: self.pass_checks.clone(),
        })
    }

    /// Convert data type of state reader, with an async closure.
    /// # Arguments
    /// * `capacity` - capacity of the new broadcast channel will be created.
    /// * `f` - an async closure, which takes the old state value as parameter, and return the new state value.
    /// # Returns
    /// Reader of new data type.
    pub fn async_entend_with<T, F, Fut>(&self, capacity: usize, f: F) -> Reader<T>
    where
        T: 'static + AsState + Send,
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
        Reader(Inner {
            capacity,
            sender: tx,
            pass_checks: self.pass_checks.clone(),
        })
    }
}
