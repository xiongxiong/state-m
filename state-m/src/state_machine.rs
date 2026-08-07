use crate::{
    AsState, AsTag, KvAssoc, State, StateEvent,
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
    ops::Deref,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;

/// StateMachine: data structure to store handles.
/// * `K` - the `Tag` type to distinguish different handles.
#[derive(Clone)]
pub struct StateMachine<K>(String, Arc<DashMap<K, (String, Box<dyn AsHandle>)>>)
where
    K: AsTag;

impl<K> Debug for StateMachine<K>
where
    K: AsTag,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for item in self.iter().sorted_by_key(|k| k.key().clone()) {
            let (_, (l, h)) = item.pair();
            write!(f, "{:<20} | {:?}\n", l, h.debug_state())?
        }
        Ok(())
    }
}

impl<K> Default for StateMachine<K>
where
    K: AsTag,
{
    fn default() -> Self {
        Self(Default::default(), Default::default())
    }
}

impl<K> Deref for StateMachine<K>
where
    K: AsTag,
{
    type Target = Arc<DashMap<K, (String, Box<dyn AsHandle>)>>;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    fn get_handle<T>(&self, tag: T) -> Result<ArcHandle<T>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync,
    {
        let k = tag.clone().into();
        match self.get(&k) {
            Some(v) => match v.value().1.downcast_ref::<ArcHandle<T>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag.into())),
        }
    }

    /// Create a state machine.
    /// * `id` - used to distinguish between different log messages.
    pub fn new(id: &str) -> Self {
        Self(id.into(), Default::default())
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    /// Add state source into state machine.
    pub async fn add_source<T>(
        &self,
        tag: T,
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Result<(), AddHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag.into()));
        }
        let h = ArcHandle(Arc::new(Handle::from_source(
            self.0.clone(),
            tag.clone(),
            Source::<T::Value>::new(capacity, pass_checks),
        )));
        h.init().await;
        self.insert(k, (format!("{tag:?}"), Box::new(h)));
        Ok(())
    }

    /// Add state reader into state machine.
    pub async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
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
        self.insert(k, (format!("{tag:?}"), Box::new(h)));
        Ok(())
    }

    /// Remove state source from state machine.
    pub fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync,
    {
        match self.remove(&tag.clone().into()) {
            Some((_, v)) => match v.1.downcast_ref::<ArcHandle<T>>() {
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
        self.contains_key(&tag.clone().into())
    }

    pub fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.reader())
    }

    pub fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.value())
    }

    pub fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.state())
    }

    pub async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.touch().await?)
    }

    pub async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.wait_touch().await?)
    }

    pub async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.cmp_alter(s, s_cmp).await?)
    }

    pub async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.wait_cmp_alter(s, s_cmp).await?)
    }

    pub async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.amend(f).await?)
    }

    pub async fn cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.cmp_amend(f, s_cmp).await?)
    }

    pub async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.wait_amend(f).await?)
    }

    pub async fn wait_cmp_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
        s_cmp: T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        Ok(self.get_handle(tag)?.wait_cmp_amend(f, s_cmp).await?)
    }

    pub fn debug_state(&self) -> Vec<String> {
        let mut states = Vec::new();
        for item in self.iter().sorted_by_key(|k| k.key().clone()) {
            let (_, (l, h)) = item.pair();
            states.push(format!("{:<20} | {:?}", l, h.debug_state()));
        }
        states
    }
}

/// State change result.
#[derive(Clone, Debug)]
pub enum StateChange<T>
where
    T: KvAssoc,
    T::Value: AsState + Send + Sync,
{
    /// State changed.
    /// * `0` - cur state.
    /// * `1` - old state.
    Change(State<T::Value>, State<T::Value>),
    /// State unchange.
    /// * `0` - cur state.
    UnChange(State<T::Value>),
}

impl<T> StateChange<T>
where
    T: KvAssoc,
    T::Value: AsState + Send + Sync,
{
    pub fn cur(&self) -> State<T::Value> {
        match self {
            StateChange::Change(v, _) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }

    pub fn old(&self) -> State<T::Value> {
        match self {
            StateChange::Change(_, v) => v.clone(),
            StateChange::UnChange(v) => v.clone(),
        }
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    sm_watch!(1);
    sm_watch!(2);
    sm_watch!(3);
    sm_watch!(4);
    sm_watch!(5);
    sm_watch!(6);
    sm_watch!(7);
    sm_watch!(8);
    sm_watch!(9);
    sm_watch!(10);

    sm_merge_reader!(2);
    sm_merge_reader!(3);
    sm_merge_reader!(4);
    sm_merge_reader!(5);
    sm_merge_reader!(6);
    sm_merge_reader!(7);
    sm_merge_reader!(8);
    sm_merge_reader!(9);
    sm_merge_reader!(10);

    sm_split_reader!(2);
    sm_split_reader!(3);
    sm_split_reader!(4);
    sm_split_reader!(5);
    sm_split_reader!(6);
    sm_split_reader!(7);
    sm_split_reader!(8);
    sm_split_reader!(9);
    sm_split_reader!(10);
}

pub trait HasStateMachine {
    type K: AsTag;

    fn state_machine(&self) -> &StateMachine<Self::K>;
}

#[async_trait]
pub trait UseStateMachine: HasStateMachine {
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Add state reader into state machine.
    /// * `tag` - the `Tag` of the reader.
    /// * `reader` - the reader to be added into state machine.
    async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Delete state handle (source or reader) from state machine.
    /// * `tag` - the `Tag` of the handle to be deleted.
    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync;

    /// If there is a handle (source or reader) in state machine for a tag.
    /// * `tag` - the `Tag` to find handle associated with it.
    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<Self::K>;

    /// Get a new reader from state machine, which will receive state change events individually.
    /// * `tag` - the `Tag` of the handle which you want to get a reader from it.
    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync;

    /// Get current state value of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state value from it.
    fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Get current state of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state from it.
    fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Trigger a state change event, but doesn't change the state value.
    /// * `tag` - the `Tag` of the handle which you want to touch.
    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Trigger a state change event, but doesn't change the state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to touch.
    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Alter a state in state machine.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Alter a state in state machine, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    /// * `s` - the state to set.
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    /// * `f` - the closure to be used on the current state to get a new state.
    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Get debug state of state machine, ordered by tag.
    fn debug_state(&self) -> Vec<String>;

    watch_decl!(1);
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
}

#[async_trait]
impl<M> UseStateMachine for M
where
    M: 'static + HasStateMachine + Send + Sync,
{
    async fn add_source<T>(
        &self,
        tag: T,
        capacity: usize,
        pass_checks: Option<Vec<Box<dyn AsPassCheck + Send + Sync>>>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine()
            .add_source(tag.clone(), capacity, pass_checks)
            .await?;
        Ok(())
    }

    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: AsState + Send + Sync,
    {
        self.state_machine().reader(tag)
    }

    async fn add_reader<T>(
        &self,
        tag: T,
        reader: Reader<T::Value>,
    ) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().add_reader(tag, reader).await?;
        Ok(())
    }

    fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().value(tag)
    }

    fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().state(tag)
    }

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().touch(tag).await
    }

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_touch(tag).await
    }

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().cmp_alter(tag, s, s_cmp).await
    }

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_cmp_alter(tag, s, s_cmp).await
    }

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().cmp_amend(tag, f, s_cmp).await
    }

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_cmp_amend(tag, f, s_cmp).await
    }

    fn debug_state(&self) -> Vec<String> {
        self.state_machine().debug_state()
    }

    watch_impl!(1);
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
}

/// StateChangeError
#[derive(Debug, Error)]
pub enum StateChangeError<T, K>
where
    T: Debug + Into<K> + KvAssoc,
    T::Value: Default,
    K: AsTag,
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
    K: AsTag,
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
    K: AsTag,
{
    #[error("State handle for tag [{0:?}] already exist.")]
    AlreadyExist(K),
    #[error("The state channel has been closed.")]
    ChannelClosed,
}
