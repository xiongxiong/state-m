use std::{pin::Pin, sync::Arc};
use tokio::sync::broadcast;

pub struct Source<T> {
    pub value: Arc<T>,
    sender: Arc<broadcast::Sender<T>>,
}

impl<T> Source<T> {
    pub fn reader(&self) -> Reader<T> {
        Reader {
            value: self.value.clone(),
            sender: self.sender.clone(),
        }
    }
}

pub struct Reader<T> {
    pub value: Arc<T>,
    sender: Arc<broadcast::Sender<T>>,
}

pub trait AsStateMachine {
    type ChangeEvent;
}

pub trait AsSubscriber<S, T> {
    fn subscribe(
        &self,
        reader: &Reader<T>,
        convert: impl Fn(S) -> Pin<Box<dyn Future<Output = T>>>,
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
