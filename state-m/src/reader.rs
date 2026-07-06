use crate::state::{State, StateEvent};
use crossfire::{
    MAsyncRx,
    mpmc::{self, List},
};
use std::fmt::Debug;
use tokio::select;

#[derive(Clone, Debug)]
pub struct Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    pub(crate) recver: MAsyncRx<List<StateEvent<S>>>,
}

impl<S> Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    pub fn extend<T>(self) -> Reader<T>
    where
        T: 'static + Clone + Debug + Default + From<S> + PartialEq + Send,
    {
        let (tx, rx) = mpmc::unbounded_async();
        let rx_o = self.recver.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    res = rx_o.recv() => {
                        match res {
                            Ok(s) => {
                                let s_new = StateEvent {
                                    state: State {
                                        value: T::from(s.state.value),
                                        timestamp: s.state.timestamp,
                                    },
                                    is_touch: s.is_touch,
                                    close_handle: s.close_handle,
                                };
                                if tx.send(s_new).is_err() {
                                    break;
                                }
                            },
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Reader { recver: rx }
    }
}

impl<S> Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Send,
{
    pub fn extend_with<T, F, Fut>(self, f: F) -> Reader<T>
    where
        T: 'static + Clone + Debug + Default + PartialEq + Send,
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send,
    {
        let (tx, rx) = mpmc::unbounded_async();
        let rx_o = self.recver.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    res = rx_o.recv() => {
                        match res {
                            Ok(s) => {
                                let s_new = StateEvent {
                                    state: State {
                                        value: f(s.state.value).await,
                                        timestamp: s.state.timestamp,
                                    },
                                    is_touch: s.is_touch,
                                    close_handle: s.close_handle,
                                };
                                if tx.send(s_new).is_err() {
                                    break;
                                }
                            },
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Reader { recver: rx }
    }
}
