use crate::{
    AsState,
    source::Inner,
    state::{State, StateEvent},
};
use state_m_macro::merge_reader;
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
}

pub fn merge_group<I, T, S>(
    readers: Vec<(I, Reader<T>)>,
    func: impl Fn(I, T) -> S + Send + 'static,
    capacity: usize,
) -> Reader<S>
where
    I: 'static + Clone + Debug + Send + Sync,
    T: AsState,
    S: AsState,
{
    let (tx, _) = channel(capacity);
    let tx_c = tx.clone();
    let mut stream_map: StreamMap<usize, _> = StreamMap::new();
    for (i, item) in readers.iter().enumerate() {
        stream_map.insert(i, BroadcastStream::new(item.1.sender.subscribe()));
    }
    let ids = readers.iter().map(|r| r.0.clone()).collect::<Vec<_>>();
    tokio::spawn(async move {
        loop {
            select! {
                res = stream_map.next() => {
                    match res {
                        Some((i, Ok(s))) => {
                            if let Some(id) = ids.get(i) {
                                tracing::trace!("rx_{} | {:?} | recv -- {:?}", i, id, s.state);
                                let s_new = StateEvent {
                                    state: State {
                                        value: func(id.clone(), s.state.value),
                                        timestamp: s.state.timestamp,
                                    },
                                    is_touch: s.is_touch,
                                    close_handle: s.close_handle,
                                };
                                if tx_c.send(s_new).is_err() {
                                    break;
                                }
                            }
                        },
                        _ => break,
                    }
                }
            }
        }
    });
    let pass_checks: Vec<_> = itertools::concat(readers.iter().map(|item| {
        item.1
            .pass_checks
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

merge_reader!(2);
merge_reader!(3);
merge_reader!(4);
merge_reader!(5);
merge_reader!(6);
merge_reader!(7);
merge_reader!(8);
merge_reader!(9);
merge_reader!(10);
