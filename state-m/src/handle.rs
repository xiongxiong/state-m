use crate::{
    reader::Reader,
    source::{Source, StateChangeError},
    state::StateEvent,
};
use crossfire::{MAsyncRx, mpmc::List};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Clone, Debug)]
pub enum Handle<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    Source(Source<S>),
    Reader(Reader<S>),
}

impl<S> Handle<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    pub(crate) fn get_source(&self) -> Result<&Source<S>, HandleError<S>> {
        match self {
            Handle::Source(source) => Ok(source),
            Handle::Reader(_) => Err(HandleError::StateReadOnly),
        }
    }

    pub(crate) fn recver(&self) -> MAsyncRx<List<StateEvent<S>>> {
        match self {
            Handle::Source(source) => source.recver.clone(),
            Handle::Reader(reader) => reader.recver.clone(),
        }
    }
}

impl<S> Handle<S>
where
    S: 'static + Clone + Debug + Default + PartialEq + Unpin,
{
    pub async fn touch(&self) -> Result<(), HandleError<S>> {
        self.get_source()?.touch().await?;
        Ok(())
    }

    pub async fn wait_touch(&self) -> Result<(), HandleError<S>> {
        self.get_source()?.wait_touch().await?;
        Ok(())
    }

    pub async fn alter(&self, s: S) -> Result<(), HandleError<S>> {
        self.get_source()?.alter(s).await?;
        Ok(())
    }

    pub async fn wait_alter(&self, s: S) -> Result<(), HandleError<S>> {
        self.get_source()?.wait_alter(s).await?;
        Ok(())
    }

    pub async fn amend(&self, f: impl FnOnce(&S) -> S) -> Result<(), HandleError<S>> {
        self.get_source()?.amend(f).await?;
        Ok(())
    }

    pub async fn wait_amend(&self, f: impl FnOnce(&S) -> S) -> Result<(), HandleError<S>> {
        self.get_source()?.wait_amend(f).await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum HandleError<S>
where
    S: Default,
{
    #[error("This state is read only.")]
    StateReadOnly,
    #[error(transparent)]
    StateChangeError(#[from] StateChangeError<S>),
}
