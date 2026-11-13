use crate::{
    AsState,
    inner::Inner,
    state::{State, StateEvent},
};
use derivative::Derivative;
use state_m_macro::influx_reader;
use std::{
    ops::Deref,
    pin::Pin,
    sync::{Arc, RwLock},
};
use tokio::{
    select,
    sync::broadcast::{Sender, channel, error::RecvError},
};
use tokio_stream::{StreamExt as _, StreamMap, wrappers::BroadcastStream};

pub type GetIni<S> = Pin<Arc<dyn Fn() -> Pin<Box<dyn Future<Output = S> + Send>> + Send + Sync>>;

/// Reader of state, to receive state change events.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Reader<S>
where
    S: AsState,
{
    pub(crate) inner: Inner<S>,
    pub(crate) cache_r: Arc<RwLock<State<S>>>,
    #[derivative(Debug = "ignore")]
    pub(crate) get_ini: GetIni<S>,
}

impl<S> Deref for Reader<S>
where
    S: AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> Reader<S>
where
    S: AsState,
{
    pub(crate) fn from(inner: Inner<S>, get_ini: GetIni<S>) -> Self {
        Self {
            inner,
            cache_r: Default::default(),
            get_ini,
        }
    }

    pub(crate) fn new(capacity: usize, sender: Sender<StateEvent<S>>, get_ini: GetIni<S>) -> Self {
        Self {
            inner: Inner {
                capacity,
                sender,
                pass_checks: Default::default(),
            },
            cache_r: Default::default(),
            get_ini,
        }
    }
}

impl<S> Reader<S>
where
    S: AsState,
{
    /// Check is the channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.sender.subscribe().is_closed()
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
        F: Fn(S) -> T + Send + Sync + 'static,
    {
        let (tx, _) = channel(self.inner.capacity);
        let tx_c = tx.clone();
        let mut rx_o = self.inner.sender.subscribe();
        let arc_f = Arc::new(f);
        let arc_f_c = arc_f.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    res = rx_o.recv() => {
                        match res {
                            Ok(s) => {
                                tracing::trace!("recv -- {:?}", s.state);
                                let s_new = StateEvent {
                                    state: State {
                                        value: arc_f.as_ref()(s.state.value),
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
        let get_ini_s = self.get_ini.clone();
        let get_ini_t: GetIni<T> = Arc::pin(move || {
            let get_ini_sc = get_ini_s.clone();
            let arc_f_cc = arc_f_c.clone();
            Box::pin(async move {
                let s = get_ini_sc.as_ref()().await;
                arc_f_cc.as_ref()(s)
            })
        });
        Reader {
            inner: Inner {
                capacity: self.inner.capacity,
                sender: tx,
                pass_checks: self.inner.pass_checks.clone(),
            },
            cache_r: Default::default(),
            get_ini: get_ini_t,
        }
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
        let (tx, _) = channel(self.inner.capacity);
        let tx_c = tx.clone();
        let mut rx_o = self.inner.sender.subscribe();
        let arc_f = Arc::new(f);
        let arc_f_c = arc_f.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    res = rx_o.recv() => {
                        match res {
                            Ok(s) => {
                                tracing::trace!("recv -- {:?}", s.state);
                                let s_new = StateEvent {
                                    state: State {
                                        value: arc_f.as_ref()(s.state.value).await,
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
        let get_ini_s = self.get_ini.clone();
        let get_ini_t: GetIni<T> = Arc::pin(move || {
            let get_ini_sc = get_ini_s.clone();
            let arc_f_cc = arc_f_c.clone();
            Box::pin(async move {
                let s = get_ini_sc.as_ref()().await;
                arc_f_cc.as_ref()(s).await
            })
        });
        Reader {
            inner: Inner {
                capacity: self.inner.capacity,
                sender: tx,
                pass_checks: self.inner.pass_checks.clone(),
            },
            cache_r: Default::default(),
            get_ini: get_ini_t,
        }
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
        stream_map.insert(i, BroadcastStream::new(item.inner.sender.subscribe()));
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
        item.inner
            .pass_checks
            .as_ref()
            .iter()
            .map(|v| v.clone())
            .collect::<Vec<_>>()
    }));
    Reader {
        inner: Inner {
            capacity,
            sender: tx,
            pass_checks: Arc::new(pass_checks),
        },
        cache_r: Default::default(),
        get_ini: Arc::pin(|| Box::pin(async move { T::default() })),
    }
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
