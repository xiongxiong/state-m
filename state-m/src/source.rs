use crate::{AsState, GetIni, State, barrier::AsPassCheck, inner::Inner, reader::Reader};
use std::{
    fmt::Debug,
    ops::Deref,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub(crate) struct Source<S>
where
    S: AsState,
{
    pub(crate) inner: Inner<S>,
    pub(crate) cache_w: Arc<RwLock<S>>,
    pub(crate) cache_r: Arc<RwLock<State<S>>>,
}

impl<S> Deref for Source<S>
where
    S: AsState,
{
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> Source<S>
where
    S: AsState,
{
    pub fn new(
        capacity: usize,
        pass_checks: Option<Vec<Arc<dyn AsPassCheck + Send + Sync>>>,
    ) -> Self {
        Self {
            inner: Inner::new(capacity, pass_checks),
            cache_w: Default::default(),
            cache_r: Default::default(),
        }
    }

    pub fn reader(&self) -> Reader<S> {
        let cache_w = self.cache_w.clone();
        let get_ini: GetIni<S> = Arc::pin(move || {
            let cache_w_c = cache_w.clone();
            Box::pin(async move { cache_w_c.read().unwrap().clone() })
        });
        Reader::from(self.inner.clone(), get_ini)
    }
}
