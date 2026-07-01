use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    any::{Any, type_name},
    cmp::Eq,
    fmt::Debug,
    hash::Hash,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    select,
    sync::{RwLock, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

type NotCheckEq = bool;

pub type Value<S> = (S, DateTime<Utc>);

#[derive(Clone, Debug)]
pub struct StateReader<S> {
    value: Arc<RwLock<Value<S>>>,
    sender: broadcast::Sender<(S, NotCheckEq, Option<mpsc::UnboundedSender<()>>)>,
}

pub struct StateSource<S>(StateReader<S>);

pub enum StateHandle<S> {
    Source(StateSource<S>),
    Reader(StateReader<S>),
}

#[derive(Clone, Debug)]
pub struct StateMachine<G>(Arc<DashMap<G, Box<dyn Any + Send + Sync>>>)
where
    G: Eq + Hash;
