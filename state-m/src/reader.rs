use crate::{
    core::{AsyncSinkWrapper, AsyncStreamWrapper},
    state::{State, StateEvent},
};
use crossfire::mpmc::{self, List};
use futures::StreamExt;
use std::fmt::Debug;

pub struct Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    pub(crate) stream: AsyncStreamWrapper<List<StateEvent<S>>>,
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
        let sink = AsyncSinkWrapper(tx.into_async().into_sink());
        let stream = self.stream.map(|s| {
            Ok(StateEvent {
                state: State {
                    value: T::from(s.state.value),
                    timestamp: s.state.timestamp,
                },
                is_touch: s.is_touch,
                close_handle: s.close_handle,
            })
        });
        tokio::spawn(stream.forward(sink));
        Reader {
            stream: AsyncStreamWrapper(rx.into_stream()),
        }
    }
}
