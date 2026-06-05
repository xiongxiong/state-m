use async_trait::async_trait;
use std::{fmt::Debug, pin::Pin, sync::Arc};
use tokio::sync::{Mutex, broadcast};

pub struct Source<S> {
    pub value: Arc<S>,
    sender: Arc<broadcast::Sender<S>>,
}

impl<S> Source<S> {
    pub fn reader(&self) -> Reader<S> {
        Reader {
            value: self.value.clone(),
            sender: self.sender.clone(),
        }
    }
}

pub struct Reader<S> {
    pub value: Arc<S>,
    sender: Arc<broadcast::Sender<S>>,
}

pub trait AsStateMachine {
    fn lock() -> Arc<Mutex<()>>;
}

#[async_trait]
pub trait AsSubscriber<S, T>: AsStateMachine
where
    T: Debug + Clone,
{
    async fn on_change(&self, new_value: T, old_value: Option<T>);

    async fn subscribe(
        &self,
        reader: &Reader<T>,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send,
    ) {
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
