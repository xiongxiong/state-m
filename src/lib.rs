use async_trait::async_trait;
use std::{fmt::Debug, pin::Pin, sync::Arc};
use tokio::{
    select,
    sync::{Mutex, RwLock, broadcast, mpsc},
};
use tracing::instrument;

pub struct Source<S> {
    pub value: Arc<S>,
    sender: Arc<broadcast::Sender<S>>,
}

impl<S> Source<S> {
    pub fn reader(&self) -> Reader<S> {
        Reader {
            sender: self.sender.clone(),
        }
    }
}

pub struct Reader<S> {
    sender: Arc<broadcast::Sender<S>>,
}

pub trait AsStateMachine {
    fn state_machine(self: Arc<Self>) -> Arc<Mutex<()>>;
}

pub trait AsSource<S> {
    /// 通道容量
    const CHAN_CAP: usize = 10;
}

pub struct EventStore<T>(pub(crate) Arc<RwLock<Option<T>>>);

impl<T> EventStore<T>
where
    T: Clone + PartialEq,
{
    pub fn new() -> Self {
        EventStore(Arc::new(RwLock::new(None)))
    }

    async fn store(&self, val: T) -> bool {
        let opt_t = Some(val);
        let res = *self.0.read().await != opt_t;
        if res {
            *self.0.write().await = opt_t;
        }
        res
    }

    pub async fn value(&self) -> Option<T> {
        (*self.0.read().await).clone()
    }
}

#[async_trait]
pub trait AsTarget<S, T, Tag>: AsStateMachine
where
    Self: 'static,
    S: 'static + Debug + Clone + Send,
    T: 'static + Debug + Clone + PartialEq + Send + Sync,
    Tag: 'static + Debug + Send,
{
    async fn on_change(self: Arc<Self>, new_value: T, old_value: Option<T>) -> anyhow::Result<()>;

    #[instrument(name = "AsTarget::subscribe", skip_all, fields(tag, chan_cap))]
    async fn subscribe(
        self: Arc<Self>,
        reader: Reader<S>,
        tag: Tag,
        chan_cap: usize,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
    ) {
        let mut rx_s = reader.sender.subscribe();
        let (tx_t, mut rx_t) = mpsc::channel::<T>(chan_cap);
        let t_store: EventStore<T> = EventStore::new();
        tokio::spawn(async move {
            tracing::info!("Subscription start -- {:?}", tag);
            loop {
                select! {
                    res = rx_s.recv() => {
                        match res {
                            Ok(s) => {
                                let t = convert(s).await;
                                if t_store.store(t.clone()).await {
                                    if let Err(e) = tx_t.send(t).await {
                                        tracing::error!("stage [2] | change event send error -- {}", e);
                                        break;
                                    }
                                }
                            },
                            Err(e) => match e {
                                broadcast::error::RecvError::Closed => {
                                    break;
                                },
                                broadcast::error::RecvError::Lagged(_) => {
                                    tracing::error!("stage [1] | change event recv lagged");
                                    break;
                                },
                            },
                        }
                    }
                    res = rx_t.recv() => {
                        match res {
                            Some(t) => {
                                let state_machine = self.clone().state_machine();
                                let _lock = state_machine.lock();
                                if let Err(e) = self.clone().on_change(t, t_store.value().await).await {
                                    tracing::error!("stage [3] | change event proc error -- {}", e);
                                }
                            },
                            None => {
                                break;
                            },
                        }
                    }
                }
            }
            tracing::info!("Subscription end -- {:?}", tag);
        });
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
