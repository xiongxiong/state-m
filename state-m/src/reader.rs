use crate::{
    AsState,
    source::Inner,
    state::{State, StateEvent},
};
use state_m_macro::influx_reader;
use std::{fmt::Debug, ops::Deref, sync::Arc};
use tokio::{
    select,
    sync::broadcast::{Sender, channel, error::RecvError},
};
use tokio_stream::{StreamExt as _, StreamMap, wrappers::BroadcastStream};

/// Reader of state, to receive state change events.
#[derive(Clone, Debug)]
pub struct Reader<S>(Inner<S>)
where
    S: AsState;

impl<S> Deref for Reader<S>
where
    S: AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> Reader<S>
where
    S: AsState,
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
    S: AsState,
{
    /// Check is the channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.sender.subscribe().is_closed()
    }

    /// Convert data type of state reader.
    /// # Arguments
    /// # Returns
    /// Reader of new data type.
    pub fn derive<T>(&self) -> Reader<T>
    where
        T: AsState + From<S> + Send,
    {
        self.derive_by(T::from)
    }

    /// Convert data type of state reader, with an closure.
    /// # Arguments
    /// * `f` - an closure, which takes the old state value as parameter, and return the new state value.
    /// # Returns
    /// Reader of new data type.
    pub fn derive_by<T, F>(&self, f: F) -> Reader<T>
    where
        T: AsState,
        F: Fn(S) -> T + Send + 'static,
    {
        let (tx, _) = channel(self.capacity);
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
                                        tracing::warn!("Lagged, skipped {n} messages.")
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
    /// * `f` - an async closure, which takes the old state value as parameter, and return the new state value.
    /// # Returns
    /// Reader of new data type.
    pub fn derive_by_async<T, F, Fut>(&self, f: F) -> Reader<T>
    where
        T: AsState,
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send,
    {
        let (tx, _) = channel(self.capacity);
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
                                        tracing::warn!("Lagged | skipped {n} messages.")
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
}

/// Influx of a group of readers.
pub fn influx_group<T>(readers: Vec<Reader<T>>, capacity: usize) -> Reader<T>
where
    T: AsState,
{
    let (tx, _) = channel(capacity);
    let tx_c = tx.clone();
    let mut stream_map: StreamMap<usize, _> = StreamMap::new();
    for (i, item) in readers.iter().enumerate() {
        stream_map.insert(i, BroadcastStream::new(item.sender.subscribe()));
    }
    tokio::spawn(async move {
        loop {
            select! {
                res = stream_map.next() => {
                    match res {
                        Some((_, Ok(s))) => {
                            if tx_c.send(s).is_err() {
                                break;
                            }
                        },
                        _ => break,
                    }
                }
            }
        }
    });
    let pass_checks: Vec<_> = itertools::concat(readers.iter().map(|item| {
        item.pass_checks
            .as_ref()
            .iter()
            .map(|v| v.clone())
            .collect::<Vec<_>>()
    }));
    Reader(Inner {
        capacity,
        sender: tx,
        pass_checks: Arc::new(pass_checks),
    })
}

influx_reader!(2);
influx_reader!(3);
influx_reader!(4);
influx_reader!(5);
influx_reader!(6);
influx_reader!(7);
influx_reader!(8);
influx_reader!(9);
influx_reader!(10);
