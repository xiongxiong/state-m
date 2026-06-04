use async_trait::async_trait;
use std::{pin::Pin, sync::Arc};
use tokio::sync::broadcast;

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
    type ChangeEvent;
}

#[async_trait]
pub trait AsSubscriber<S, T>: AsStateMachine
where
    T: Into<Self::ChangeEvent>,
{
    async fn subscribe(
        &self,
        reader: &Reader<T>,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T> + Send>> + Send,
    ) {
        //
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
