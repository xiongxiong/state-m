use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crossfire::{
    MAsyncRx, MAsyncTx, MTx,
    mpmc::{List, Null},
    null::CloseHandle,
};
use dashmap::DashMap;
use derivative::Derivative;
use std::{
    any::{Any, type_name},
    cmp::Eq,
    fmt::Debug,
    hash::Hash,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::{select, sync::RwLock};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct State<S>
where
    S: Default,
{
    value: S,
    not_check_eq: bool,
    #[derivative(Debug = "ignore")]
    close_handle: Option<CloseHandle<Null>>,
    timestamp: DateTime<Utc>,
}

impl<S> Default for State<S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            value: Default::default(),
            not_check_eq: Default::default(),
            close_handle: Default::default(),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    value: Arc<RwLock<State<S>>>,
    sender: MAsyncTx<List<State<S>>>,
    recver: MAsyncRx<List<State<S>>>,
}

impl<S> Reader<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    fn new(init_value: S) -> Self {
        Self {
            value: Arc::new(RwLock::new(v)),
            sender: todo!(),
            recver: todo!(),
        }
    }
}

pub struct Source<S>(Reader<S>)
where
    S: 'static + Clone + Debug + Default + PartialEq;

impl<S> Source<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    fn new(init_value: S) -> Self {
        Self(Reader::new(init_value))
    }
}

pub enum Handle<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    Source(Source<S>),
    Reader(Reader<S>),
}

#[derive(Clone, Debug)]
pub struct StateMachine<G>(Arc<DashMap<G, Box<dyn Any + Send + Sync>>>)
where
    G: Eq + Hash;
