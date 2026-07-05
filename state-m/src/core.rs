use crossfire::{TrySendError, flavor::Flavor, sink::AsyncSink, stream::AsyncStream};
use futures::{Sink, Stream};
use std::{
    ops::{Deref, DerefMut},
    task::Poll,
};

pub(crate) struct AsyncStreamWrapper<F: Flavor>(pub AsyncStream<F>);

impl<F> Deref for AsyncStreamWrapper<F>
where
    F: Flavor,
{
    type Target = AsyncStream<F>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F> DerefMut for AsyncStreamWrapper<F>
where
    F: Flavor,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<F> Stream for AsyncStreamWrapper<F>
where
    F: Flavor,
{
    type Item = F::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().poll_item(cx)
    }
}

pub(crate) struct AsyncSinkWrapper<F: Flavor>(pub AsyncSink<F>);

impl<F> Sink<F::Item> for AsyncSinkWrapper<F>
where
    F: Flavor,
{
    type Error = TrySendError<F::Item>;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: F::Item) -> Result<(), Self::Error> {
        self.0.try_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
