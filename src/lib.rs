use async_trait::async_trait;
use std::{fmt::Debug, pin::Pin, sync::Arc};
use tokio::{
    select,
    sync::{Mutex, broadcast, mpsc},
};

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
    fn state_machine() -> Arc<Mutex<()>>;
}

pub trait AsSource<S> {
    /// 通道容量
    const CHAN_CAP: usize = 10;
}

#[async_trait]
pub trait AsTarget<S, T>: AsStateMachine
where
    S: 'static + Debug + Clone + Send,
    T: 'static + Debug + Clone + Send,
{
    /// 通道容量
    const CHAN_CAP: usize = 10;

    async fn on_change(&self, new_value: T, old_value: Option<T>);

    async fn subscribe(
        &self,
        reader: &Reader<S>,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send,
    ) {
        let mut rx_s = reader.sender.subscribe();
        tokio::spawn(async move {
            loop {
                select! {
                    v = rx_s.recv() => {

                    }
                }
            }
        });
        let (tx, rx) = mpsc::channel::<T>(Self::CHAN_CAP);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
