use crate::{
    AsKey, AsState, IsHidden, KeyIsTag, KvAssoc, State, StateEvent,
    barrier::AsPassCheck,
    handle::{ArcHandle, AsHandle, Handle, StateChangeError as HandleStateChangeError},
    reader::Reader,
    source::Source,
};
use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use state_m_macro::*;
use std::{
    fmt::{self, Debug},
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio_stream::{StreamExt, StreamMap, wrappers::BroadcastStream};
use tokio_util::sync::CancellationToken;

/// The id of state machine.
type Id = String;
/// If hide handle's debug_state or not
type Hidden = bool;
/// The label of handle.
type Label = String;

/// StateMachine: data structure to store handles.
/// * `K` - the `Tag` type to distinguish different handles.
#[derive(Clone)]
pub struct StateMachine<K>(Id, Arc<DashMap<K, (Label, Hidden, Box<dyn AsHandle>)>>)
where
    K: AsKey;

impl<K> Debug for StateMachine<K>
where
    K: AsKey,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for item in self.1.iter().sorted_by_key(|k| k.key().clone()) {
            let (_, (label, _, handle)) = item.pair();
            write!(f, "{:<20} | {:?}\n", label, handle.debug_state())?
        }
        Ok(())
    }
}

impl<K> Default for StateMachine<K>
where
    K: AsKey,
{
    fn default() -> Self {
        Self(Default::default(), Default::default())
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsKey,
{
    fn get_handle<T>(&self, tag: T) -> Result<ArcHandle<T>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        let k = tag.clone().into();
        match self.1.get(&k) {
            Some(v) => match v.value().2.downcast_ref::<ArcHandle<T>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag.into())),
        }
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsKey,
{
    /// Create a state machine.
    /// * `id` - used to distinguish between different log messages.
    pub fn new(id: &str) -> Self {
        Self(id.into(), Default::default())
    }

    /// Judge if a Tag for state machine is a Source or Reader.
    /// * `tag` - the `Tag` of the source.
    pub fn is_source<T>(&self, tag: T) -> Result<bool, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.get_handle(tag).map(|v| v.is_source())
    }

    /// Remove state source from state machine.
    pub fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        match self.1.remove(&tag.clone().into()) {
            Some((_, v)) => match v.2.downcast_ref::<ArcHandle<T>>() {
                Some(h) => {
                    h.close();
                    Ok(true)
                }
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Ok(false),
        }
    }

    /// If state source of tag exists in state machine.
    pub fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.1.contains_key(&tag.clone().into())
    }

    pub fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.reader())
    }

    pub fn keys(&self) -> Vec<K> {
        self.1.iter().map(|v| v.key().clone()).sorted().collect()
    }

    pub fn tags<T>(&self) -> Vec<T>
    where
        K: KeyIsTag<T>,
        T: Into<K> + TryFrom<K, Error = &'static str>,
    {
        self.1
            .iter()
            .filter(|v| KeyIsTag::<T>::predicate(v.key()))
            .map(|v| T::try_from(v.key().clone()).unwrap())
            .collect::<Vec<_>>()
    }

    pub fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.value())
    }

    pub fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.state())
    }

    pub fn debug_state(&self) -> Vec<String> {
        let mut states = Vec::new();
        for item in self.1.iter().sorted_by_key(|k| k.key().clone()) {
            let (_, (label, hidden, handle)) = item.pair();
            if !hidden {
                states.push(format!("{:<20} | {:?}", label, handle.debug_state()));
            }
        }
        states
    }

    pub async fn add_source<T>(
        &self,
        tag: T,
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Result<(), AddHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        let k = tag.clone().into();
        if self.1.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag.into()));
        }
        let h = ArcHandle(Arc::new(Handle::from_source(
            self.0.clone(),
            tag.clone(),
            Source::<T::Value>::new(capacity, pass_checks),
        )));
        h.init().await;
        self.1
            .insert(k, (format!("{tag:?}"), T::HIDDEN, Box::new(h)));
        Ok(())
    }

    pub async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        let k = tag.clone().into();
        if self.1.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag.into()));
        }
        if reader.is_closed() {
            return Err(AddHandleError::ChannelClosed);
        }
        let h = ArcHandle(Arc::new(Handle::from_reader(
            self.0.clone(),
            tag.clone(),
            reader,
        )));
        h.init().await;
        self.1
            .insert(k, (format!("{tag:?}"), T::HIDDEN, Box::new(h)));
        Ok(())
    }

    pub async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.touch().await?)
    }

    pub async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.wait_touch().await?)
    }

    pub async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.alter(s).await?)
    }

    pub async fn cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.cmp_alter(s, s_cmp).await?)
    }

    pub async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.wait_alter(s).await?)
    }

    pub async fn wait_cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.wait_cmp_alter(s, s_cmp).await?)
    }

    pub async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.amend(f).await?)
    }

    pub async fn cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.cmp_amend(f, s_cmp).await?)
    }

    pub async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.wait_amend(f).await?)
    }

    pub async fn wait_cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.wait_cmp_amend(f, s_cmp).await?)
    }

    pub async fn watch<T, F>(&self, tag: T, func: F) -> Result<CancellationToken, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
        F: 'static
            + Fn(
                (T, StateChange<T::Value>),
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        let id = self.0.clone();
        let cancel_token = CancellationToken::new();
        let cancel_token_c = cancel_token.clone();
        let handle = self.get_handle(tag.clone())?;
        let (mut rx, token) = handle.fanout();
        tokio::spawn(async move {
            tracing::info!("{id} | {tag:?} -- start");
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token_c.cancelled() => break,
                    _ = token.cancelled() => break,
                    r = rx.recv() => match r {
                        Ok((s_cur, s_old)) => {
                            if let Err(e) = func((tag.clone(), StateChange::Change(s_cur, s_old))).await {
                                tracing::error!("{id} | {:?} | watch error -- {e:?}", tag);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            tracing::info!("{id} | {tag:?} -- close");
        });
        Ok(cancel_token)
    }

    pub async fn watch_group<T, F>(
        &self,
        func: F,
    ) -> Result<CancellationToken, TagGroupUsageError<K>>
    where
        K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        F: 'static
            + Fn(
                Vec<(T, StateChange<T::Value>)>,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        let tags = self
            .1
            .iter()
            .filter(|v| KeyIsTag::<T>::predicate(v.key()))
            .map(|v| T::try_from(v.key().clone()).unwrap())
            .collect::<Vec<_>>();
        if tags.is_empty() {
            return Err(TagGroupUsageError::NoTagOfTyp(
                std::any::type_name::<T>().into(),
            ));
        }
        let handles: Vec<_> = tags
            .iter()
            .map(|t| self.get_handle(t.clone()).unwrap())
            .collect();
        let mut stream_map: StreamMap<T, _> = StreamMap::new();
        for (t, h) in tags.iter().zip(handles.iter()) {
            stream_map.insert((*t).clone(), BroadcastStream::new(h.fanout().0));
        }
        let get_all_states = move || {
            let states = tags
                .iter()
                .zip(handles.iter())
                .map(|(t, h)| (t.clone(), StateChange::UnChange(h.state())))
                .collect::<Vec<_>>();
            states
        };
        let id = self.0.clone();
        let cancel_token = CancellationToken::new();
        let cancel_token_c = cancel_token.clone();
        tokio::spawn(async move {
            tracing::info!("{id} | watch_group -- start");
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token_c.cancelled() => break,
                    r = stream_map.next() => {
                        match r {
                            Some((t, Ok((s_cur, s_old)))) => {
                                let states = get_all_states().iter().map(|(t_i, c_i)| {
                                    if *t_i == t {
                                        (t.clone(), StateChange::Change(s_cur.clone(), s_old.clone()))
                                    } else {
                                        (t_i.clone(), c_i.clone())
                                    }
                                }).collect::<Vec<_>>();
                                if let Err(e) = func(states).await {
                                    tracing::error!("{id} | {:?} | watch_group error -- {e:?}", t);
                                }
                            }
                            _ => break
                        }
                    }
                }
            }
            tracing::info!("{id} | watch_group -- close");
        });
        Ok(cancel_token)
    }

    pub async fn merge_group<T, S, F>(
        &self,
        capacity: usize,
        func: F,
    ) -> Result<Reader<S>, TagGroupUsageError<K>>
    where
        K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        S: AsState,
        F: 'static + Fn(Vec<(T, T::Value)>) -> S + Send,
    {
        let tags = self
            .1
            .iter()
            .filter(|v| KeyIsTag::<T>::predicate(v.key()))
            .map(|v| T::try_from(v.key().clone()).unwrap())
            .collect::<Vec<_>>();
        if tags.is_empty() {
            return Err(TagGroupUsageError::NoTagOfTyp(
                std::any::type_name::<T>().into(),
            ));
        }
        let handles: Vec<_> = tags
            .iter()
            .map(|t| self.get_handle(t.clone()).unwrap())
            .collect();
        let mut stream_map: StreamMap<T, _> = StreamMap::new();
        for (t, h) in tags.iter().zip(handles.iter()) {
            stream_map.insert((*t).clone(), BroadcastStream::new(h.fanout().0));
        }
        let get_all_states = move || {
            tags.iter()
                .zip(handles.iter())
                .map(|(t, h)| ((*t).clone(), h.value()))
                .collect::<Vec<_>>()
        };
        let id = self.0.clone();
        let (tx, _) = tokio::sync::broadcast::channel(capacity);
        let tx_c = tx.clone();
        tokio::spawn(async move {
            tracing::info!("{id} | merge_group -- start");
            loop {
                tokio::select! {
                    r = stream_map.next() => {
                        match r {
                            Some((_, Ok(_))) => {
                                let states = get_all_states();
                                let value = func(states);
                                let event = StateEvent {
                                    state: State {
                                        value,
                                        timestamp: chrono::Utc::now(),
                                    },
                                    is_touch: false,
                                    close_handle: None,
                                };
                                if tx_c.send(event).is_err() {
                                    break;
                                }
                            }
                            _ => break
                        }
                    }
                }
            }
            tracing::info!("{id} | merge_group -- close");
        });
        Ok(Reader::new(capacity, tx))
    }

    watch_sm!(2);
    watch_sm!(3);
    watch_sm!(4);
    watch_sm!(5);
    watch_sm!(6);
    watch_sm!(7);
    watch_sm!(8);
    watch_sm!(9);
    watch_sm!(10);

    merge_reader_sm!(2);
    merge_reader_sm!(3);
    merge_reader_sm!(4);
    merge_reader_sm!(5);
    merge_reader_sm!(6);
    merge_reader_sm!(7);
    merge_reader_sm!(8);
    merge_reader_sm!(9);
    merge_reader_sm!(10);

    split_reader_sm!(2);
    split_reader_sm!(3);
    split_reader_sm!(4);
    split_reader_sm!(5);
    split_reader_sm!(6);
    split_reader_sm!(7);
    split_reader_sm!(8);
    split_reader_sm!(9);
    split_reader_sm!(10);

    watch_group_sm!(2);
    watch_group_sm!(3);
    watch_group_sm!(4);
    watch_group_sm!(5);
    watch_group_sm!(6);
    watch_group_sm!(7);
    watch_group_sm!(8);
    watch_group_sm!(9);
    watch_group_sm!(10);

    merge_group_sm!(2);
    merge_group_sm!(3);
    merge_group_sm!(4);
    merge_group_sm!(5);
    merge_group_sm!(6);
    merge_group_sm!(7);
    merge_group_sm!(8);
    merge_group_sm!(9);
    merge_group_sm!(10);
}

/// State change result.
#[derive(Clone, Debug)]
pub enum StateChange<T>
where
    T: AsState,
{
    /// State changed.
    /// * `0` - cur state.
    /// * `1` - old state.
    Change(State<T>, State<T>),
    /// State unchange.
    /// * `0` - cur state.
    UnChange(State<T>),
}

impl<T> StateChange<T>
where
    T: AsState,
{
    pub fn cur(&self) -> State<T> {
        match self {
            StateChange::Change(v, _) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }

    pub fn old(&self) -> State<T> {
        match self {
            StateChange::Change(_, v) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }
}

pub trait HasStateMachine {
    type K: 'static + AsKey;

    fn state_machine(&self) -> &StateMachine<Self::K>;
}

#[async_trait]
pub trait UseStateMachine: HasStateMachine {
    /// Judge if a Tag for state machine is a Source or Reader.
    /// * `tag` - the `Tag` of the source.
    fn is_source<T>(&self, tag: T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Add state source into state machine.
    /// * `tag` - the `Tag` of the source.
    /// * `capacity` - the capacity of broadcast channel.
    /// * `pass_checks` - check conditions to continue to recieve state change events.
    async fn add_source<T>(
        &self,
        tag: T,
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Add state reader into state machine.
    /// * `tag` - the `Tag` of the reader.
    /// * `reader` - the reader to be added into state machine.
    async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Delete state handle (source or reader) from state machine.
    /// * `tag` - the `Tag` of the handle to be deleted.
    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// If there is a handle (source or reader) in state machine for a tag.
    /// * `tag` - the `Tag` to find handle associated with it.
    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>;

    /// Get a new reader from state machine, which will receive state change events individually.
    /// * `tag` - the `Tag` of the handle which you want to get a reader from it.
    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Get all keys of state machine.
    fn keys(&self) -> Vec<Self::K>;

    /// Get all tags of type T.
    fn tags<T>(&self) -> Vec<T>
    where
        Self::K: KeyIsTag<T>,
        T: Into<Self::K> + TryFrom<Self::K, Error = &'static str>;

    /// Get current state value of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state value from it.
    fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Get current state of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state from it.
    fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Trigger a state change event, but doesn't change the state value.
    /// * `tag` - the `Tag` of the handle which you want to touch.
    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Trigger a state change event, but doesn't change the state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to touch.
    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Alter a state in state machine.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Alter a state in state machine.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    /// * `s_cmp` - the state to compare before set, if it is not the same as the current state, state will stay untouched.
    async fn cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Alter a state in state machine, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Alter a state in state machine, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    /// * `s_cmp` - the state to compare before set, if it is not the same as the current state, state will stay untouched.
    async fn wait_cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    /// * `s_cmp` - the state to compare before set, if it is not the same as the current state, state will stay untouched.
    async fn cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    /// * `s_cmp` - the state to compare before set, if it is not the same as the current state, state will stay untouched.
    async fn wait_cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState;

    /// Get debug state of state machine, ordered by tag.
    fn debug_state(&self) -> Vec<String>;

    /// Watch a state handle, responding to state change events.
    /// * `tag` - the 'Tag' of the state handle.
    /// * `func` - the function used to handle state change events.
    async fn watch<T, F>(
        &self,
        tag: T,
        func: F,
    ) -> Result<CancellationToken, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
        F: 'static
            + Fn(
                (T, StateChange<T::Value>),
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send;

    /// Watch states of tags in the dimension of tag type.
    /// * `func` - how to respond to state change events of tags of the same tag type.
    async fn watch_group<T, F>(
        &self,
        func: F,
    ) -> Result<CancellationToken, TagGroupUsageError<Self::K>>
    where
        Self::K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<Self::K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<Self::K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        F: 'static
            + Fn(
                Vec<(T, StateChange<T::Value>)>,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send;

    /// Merge states of tags in the dimension of tag type.
    /// * `capacity` - the capacity of new state channel.
    /// * `func` - how to merge states of tags of the same tag type.
    async fn merge_group<T, S, F>(
        &self,
        capacity: usize,
        func: F,
    ) -> Result<Reader<S>, TagGroupUsageError<Self::K>>
    where
        Self::K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<Self::K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<Self::K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        S: AsState,
        F: 'static + Fn(Vec<(T, T::Value)>) -> S + Send;

    watch_decl!(2);
    watch_decl!(3);
    watch_decl!(4);
    watch_decl!(5);
    watch_decl!(6);
    watch_decl!(7);
    watch_decl!(8);
    watch_decl!(9);
    watch_decl!(10);

    merge_reader_decl!(2);
    merge_reader_decl!(3);
    merge_reader_decl!(4);
    merge_reader_decl!(5);
    merge_reader_decl!(6);
    merge_reader_decl!(7);
    merge_reader_decl!(8);
    merge_reader_decl!(9);
    merge_reader_decl!(10);

    split_reader_decl!(2);
    split_reader_decl!(3);
    split_reader_decl!(4);
    split_reader_decl!(5);
    split_reader_decl!(6);
    split_reader_decl!(7);
    split_reader_decl!(8);
    split_reader_decl!(9);
    split_reader_decl!(10);

    watch_group_decl!(2);
    watch_group_decl!(3);
    watch_group_decl!(4);
    watch_group_decl!(5);
    watch_group_decl!(6);
    watch_group_decl!(7);
    watch_group_decl!(8);
    watch_group_decl!(9);
    watch_group_decl!(10);

    merge_group_decl!(2);
    merge_group_decl!(3);
    merge_group_decl!(4);
    merge_group_decl!(5);
    merge_group_decl!(6);
    merge_group_decl!(7);
    merge_group_decl!(8);
    merge_group_decl!(9);
    merge_group_decl!(10);
}

#[async_trait]
impl<M> UseStateMachine for M
where
    M: 'static + HasStateMachine + Send + Sync,
{
    fn is_source<T>(&self, tag: T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().is_source(tag)
    }

    async fn add_source<T>(
        &self,
        tag: T,
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine()
            .add_source(tag.clone(), capacity, pass_checks)
            .await?;
        Ok(())
    }

    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().del_handle(tag)
    }

    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>,
    {
        self.state_machine().has_handle(tag)
    }

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().reader(tag)
    }

    async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().add_reader(tag, reader).await?;
        Ok(())
    }

    fn keys(&self) -> Vec<Self::K> {
        self.state_machine().keys()
    }

    fn tags<T>(&self) -> Vec<T>
    where
        Self::K: KeyIsTag<T>,
        T: Into<Self::K> + TryFrom<Self::K, Error = &'static str>,
    {
        self.state_machine().tags::<T>()
    }

    fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().value(tag)
    }

    fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().state(tag)
    }

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().touch(tag).await
    }

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().wait_touch(tag).await
    }

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().alter(tag, s).await
    }

    async fn cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().cmp_alter(tag, s, s_cmp).await
    }

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().wait_alter(tag, s).await
    }

    async fn wait_cmp_alter<T>(
        &self,
        tag: T,
        s: T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().wait_cmp_alter(tag, s, s_cmp).await
    }

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().amend(tag, f).await
    }

    async fn cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().cmp_amend(tag, f, s_cmp).await
    }

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().wait_amend(tag, f).await
    }

    async fn wait_cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
    {
        self.state_machine().wait_cmp_amend(tag, f, s_cmp).await
    }

    fn debug_state(&self) -> Vec<String> {
        self.state_machine().debug_state()
    }

    async fn watch<T, F>(
        &self,
        tag: T,
        func: F,
    ) -> Result<CancellationToken, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + IsHidden + KvAssoc + Send + Sync,
        T::Value: AsState,
        F: 'static
            + Fn(
                (T, StateChange<T::Value>),
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.state_machine().watch(tag, func).await
    }

    async fn watch_group<T, F>(
        &self,
        func: F,
    ) -> Result<CancellationToken, TagGroupUsageError<Self::K>>
    where
        Self::K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<Self::K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<Self::K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        F: 'static
            + Fn(
                Vec<(T, StateChange<T::Value>)>,
            ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
            + Send,
    {
        self.state_machine().watch_group::<T, _>(func).await
    }

    async fn merge_group<T, S, F>(
        &self,
        capacity: usize,
        func: F,
    ) -> Result<Reader<S>, TagGroupUsageError<Self::K>>
    where
        Self::K: KeyIsTag<T>,
        T: 'static
            + Clone
            + Debug
            + Eq
            + std::hash::Hash
            + Into<Self::K>
            + IsHidden
            + KvAssoc
            + PartialEq
            + Send
            + Sync
            + TryFrom<Self::K, Error = &'static str>
            + Unpin,
        T::Value: AsState,
        S: AsState,
        F: 'static + Fn(Vec<(T, T::Value)>) -> S + Send,
    {
        self.state_machine().merge_group(capacity, func).await
    }

    watch_impl!(2);
    watch_impl!(3);
    watch_impl!(4);
    watch_impl!(5);
    watch_impl!(6);
    watch_impl!(7);
    watch_impl!(8);
    watch_impl!(9);
    watch_impl!(10);

    merge_reader_impl!(2);
    merge_reader_impl!(3);
    merge_reader_impl!(4);
    merge_reader_impl!(5);
    merge_reader_impl!(6);
    merge_reader_impl!(7);
    merge_reader_impl!(8);
    merge_reader_impl!(9);
    merge_reader_impl!(10);

    split_reader_impl!(2);
    split_reader_impl!(3);
    split_reader_impl!(4);
    split_reader_impl!(5);
    split_reader_impl!(6);
    split_reader_impl!(7);
    split_reader_impl!(8);
    split_reader_impl!(9);
    split_reader_impl!(10);

    watch_group_impl!(2);
    watch_group_impl!(3);
    watch_group_impl!(4);
    watch_group_impl!(5);
    watch_group_impl!(6);
    watch_group_impl!(7);
    watch_group_impl!(8);
    watch_group_impl!(9);
    watch_group_impl!(10);

    merge_group_impl!(2);
    merge_group_impl!(3);
    merge_group_impl!(4);
    merge_group_impl!(5);
    merge_group_impl!(6);
    merge_group_impl!(7);
    merge_group_impl!(8);
    merge_group_impl!(9);
    merge_group_impl!(10);
}

/// StateChangeError
#[derive(Debug, Error)]
pub enum StateChangeError<T, K>
where
    T: Debug + Into<K> + KvAssoc,
    T::Value: Default,
    K: AsKey,
{
    #[error(transparent)]
    GetHandleError(#[from] GetHandleError<K>),
    #[error(transparent)]
    StateChangeError(#[from] HandleStateChangeError<T::Value>),
}

/// GetHandleError
#[derive(Debug, Error)]
pub enum GetHandleError<K>
where
    K: AsKey,
{
    #[error("State handle for tag [{0:?}] not exist.")]
    HandleNotExist(K),
    #[error("Type of state value does not match.")]
    TypeNotMatch,
}

/// AddHandleError
#[derive(Debug, Error)]
pub enum AddHandleError<K>
where
    K: AsKey,
{
    #[error("State handle for tag [{0:?}] already exist.")]
    AlreadyExist(K),
    #[error("The state channel has been closed.")]
    ChannelClosed,
}

#[derive(Debug, Error)]
pub enum TagUsageError<K>
where
    K: AsKey,
{
    #[error("Should not use duplicate tags.")]
    DuplicatedTags,
    #[error(transparent)]
    GetHandleError(#[from] GetHandleError<K>),
}

#[derive(Debug, Error)]
pub enum TagGroupUsageError<K>
where
    K: AsKey,
{
    #[error("Should not use duplicate tag types.")]
    DuplicatedTagTyps,
    #[error("There is no tag of type '{_0}'")]
    NoTagOfTyp(String),
    #[error(transparent)]
    TagUsageError(#[from] TagUsageError<K>),
}
