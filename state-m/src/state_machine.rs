use crate::{
    AsState, AsTag, KvAssoc, State, StateEvent,
    handle::{Handle, StateChangeError as HandleStateChangeError},
    reader::Reader,
    source::Source,
};
use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use state_m_macro::*;
use std::{any::Any, fmt::Debug, ops::Deref, pin::Pin, sync::Arc};
use thiserror::Error;
use tracing::instrument;

/// StateMachine: data structure to store handles.
/// * `K` - the `Tag` type to distinguish different handles.
#[derive(Clone, Debug)]
pub struct StateMachine<K>(Arc<DashMap<K, Box<dyn Any + Send + Sync>>>)
where
    K: AsTag;

impl<K> Default for StateMachine<K>
where
    K: AsTag,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K> Deref for StateMachine<K>
where
    K: AsTag,
{
    type Target = Arc<DashMap<K, Box<dyn Any + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    fn get_handle<T>(&self, tag: T) -> Result<Arc<Handle<T::Value>>, GetHandleError<K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsState,
    {
        let k = tag.clone().into();
        match self.get(&k) {
            Some(v) => match v.downcast_ref::<Arc<Handle<T::Value>>>() {
                Some(h) => Ok(h.clone()),
                None => Err(GetHandleError::TypeNotMatch),
            },
            None => Err(GetHandleError::HandleNotExist(tag.into())),
        }
    }
}

impl<K> StateMachine<K>
where
    K: 'static + AsTag,
{
    /// Add state source into state machine.
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        let k = tag.clone().into();
        if self.contains_key(&k) {
            return Err(AddHandleError::AlreadyExist(tag.into()));
        }
        let h = Arc::new(Handle::from_source(Source::<T::Value>::new(capacity)));
        h.init(tag).await;
        self.insert(k, Box::new(h));
        Ok(())
    }

    /// Add state reader into state machine.
    async fn add_reader<T>(&self, tag: T, reader: Reader<T::Value>) -> Result<(), AddHandleError<K>>
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
        let h = Arc::new(Handle::from_reader(reader));
        h.init(tag).await;
        self.insert(k, Box::new(h));
        Ok(())
    }

    /// Remove state source from state machine.
    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<K>>
    where
        T: 'static + Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsState,
    {
        match self.remove(&tag.clone().into()) {
            Some((_, v)) => match v.downcast_ref::<Arc<Handle<T::Value>>>() {
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
    fn has_handle<T>(&self, tag: &T) -> bool
    where
        T: Clone + Into<K>,
    {
        self.contains_key(&tag.clone().into())
    }

    fn reader<T>(&self, tag: T) -> Result<Reader<T::Value>, GetHandleError<K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: AsState,
    {
        Ok(self.get_handle(tag)?.reader())
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.value().await)
    }

    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.state().await)
    }

    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.touch().await?)
    }

    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_touch().await?)
    }

    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.alter(s).await?)
    }

    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_alter(s).await?)
    }

    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.amend(f).await?)
    }

    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value,
    ) -> Result<(), StateChangeError<T, K>>
    where
        T: Clone + Debug + Into<K> + KvAssoc,
        T::Value: 'static + AsState,
    {
        Ok(self.get_handle(tag)?.wait_amend(f).await?)
    }

    // async fn split_reader<T, F, S0, S1>(
    //     &self,
    //     tag: T,
    //     func: F,
    // ) -> Result<(Reader<S0>, Reader<S1>), GetHandleError<K>>
    // where
    //     T: Clone + Debug + Into<K> + KvAssoc,
    //     T::Value: 'static + AsState + Send,
    //     F: 'static + Fn(T::Value) -> (S0, S1) + Send,
    //     S0: 'static + AsState + Send,
    //     S1: 'static + AsState + Send,
    // {
    //     let handle = self.get_handle(tag)?;
    //     let capacity = handle.capacity();
    //     let (mut rx, token) = handle.fanout();
    //     let (tx_0, _) = tokio::sync::broadcast::channel(capacity);
    //     let (tx_1, _) = tokio::sync::broadcast::channel(capacity);
    //     let tx_0_c = tx_0.clone();
    //     let tx_1_c = tx_1.clone();
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::select! {
    //                 biased;
    //                 _ = token.cancelled() => break,
    //                 r = rx.recv() => {
    //                     match r {
    //                         Ok((s_cur, _)) => {
    //                             let (v_0, v_1) = func(s_cur.value);
    //                             let s_0 = StateEvent {
    //                                 state: State {
    //                                     value: v_0,
    //                                     timestamp: s_cur.timestamp.clone(),
    //                                 },
    //                                 is_touch: false,
    //                                 close_handle: None,
    //                             };
    //                             if tx_0_c.send(s_0).is_err() {
    //                                 break;
    //                             }
    //                             let s_1 = StateEvent {
    //                                 state: State {
    //                                     value: v_1,
    //                                     timestamp: s_cur.timestamp.clone(),
    //                                 },
    //                                 is_touch: false,
    //                                 close_handle: None,
    //                             };
    //                             if tx_1_c.send(s_1).is_err() {
    //                                 break;
    //                             }
    //                         },
    //                         Err(_) => break,
    //                     }
    //                 }
    //             }
    //         }
    //     });
    //     Ok((Reader::new(capacity, tx_0), Reader::new(capacity, tx_1)))
    // }
}

/// State change result.
#[derive(Clone, Debug)]
pub enum StateChange<T>
where
    T: KvAssoc,
    T::Value: AsState,
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
    T::Value: AsState,
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
    sm_watch!(11);
    sm_watch!(12);
    sm_watch!(13);
    sm_watch!(14);
    sm_watch!(15);
    sm_watch!(16);
    sm_watch!(17);
    sm_watch!(18);
    sm_watch!(19);
    sm_watch!(20);

    sm_merge_reader!(2);
    sm_merge_reader!(3);
    sm_merge_reader!(4);
    sm_merge_reader!(5);
    sm_merge_reader!(6);
    sm_merge_reader!(7);
    sm_merge_reader!(8);
    sm_merge_reader!(9);
    sm_merge_reader!(10);
    sm_merge_reader!(11);
    sm_merge_reader!(12);
    sm_merge_reader!(13);
    sm_merge_reader!(14);
    sm_merge_reader!(15);
    sm_merge_reader!(16);
    sm_merge_reader!(17);
    sm_merge_reader!(18);
    sm_merge_reader!(19);
    sm_merge_reader!(20);

    sm_split_reader!(2);
    sm_split_reader!(3);
    sm_split_reader!(4);
    sm_split_reader!(5);
    sm_split_reader!(6);
    sm_split_reader!(7);
    sm_split_reader!(8);
    sm_split_reader!(9);
    sm_split_reader!(10);
    sm_split_reader!(11);
    sm_split_reader!(12);
    sm_split_reader!(13);
    sm_split_reader!(14);
    sm_split_reader!(15);
    sm_split_reader!(16);
    sm_split_reader!(17);
    sm_split_reader!(18);
    sm_split_reader!(19);
    sm_split_reader!(20);
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
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<Self::K>>
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc,
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
        T: Clone + Debug + Into<Self::K> + KvAssoc,
        T::Value: AsState;

    /// Get current state value of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state value from it.
    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Get current state of a tag in state machine.
    /// * `tag` - the `Tag` of the handle which you want to get state from it.
    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
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
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Alter a state in state machine, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to alter.
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    async fn amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

    /// Amend a state in state machine, with a closure which take the current state value as parameter and return new state value, and wait for all readers finishing responding actions.
    /// * `tag` - the `Tag` of the handle which you want to amend.
    async fn wait_amend<T>(
        &self,
        tag: T,
        f: impl FnOnce(T::Value) -> T::Value + Send,
    ) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync;

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
    watch_decl!(11);
    watch_decl!(12);
    watch_decl!(13);
    watch_decl!(14);
    watch_decl!(15);
    watch_decl!(16);
    watch_decl!(17);
    watch_decl!(18);
    watch_decl!(19);
    watch_decl!(20);

    merge_reader_decl!(2);
    merge_reader_decl!(3);
    merge_reader_decl!(4);
    merge_reader_decl!(5);
    merge_reader_decl!(6);
    merge_reader_decl!(7);
    merge_reader_decl!(8);
    merge_reader_decl!(9);
    merge_reader_decl!(10);
    merge_reader_decl!(11);
    merge_reader_decl!(12);
    merge_reader_decl!(13);
    merge_reader_decl!(14);
    merge_reader_decl!(15);
    merge_reader_decl!(16);
    merge_reader_decl!(17);
    merge_reader_decl!(18);
    merge_reader_decl!(19);
    merge_reader_decl!(20);
}

#[async_trait]
impl<M> UseStateMachine for M
where
    M: 'static + HasStateMachine + Send + Sync,
{
    async fn add_source<T>(&self, tag: T, capacity: usize) -> Result<(), AddHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine()
            .add_source(tag.clone(), capacity)
            .await?;
        Ok(())
    }

    fn del_handle<T>(&self, tag: &T) -> Result<bool, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc,
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
        T: Clone + Debug + Into<Self::K> + KvAssoc,
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
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().add_reader(tag, reader).await?;
        Ok(())
    }

    async fn value<T>(&self, tag: T) -> Result<T::Value, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().value(tag).await
    }

    async fn state<T>(&self, tag: T) -> Result<State<T::Value>, GetHandleError<Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().state(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_touch<T>(&self, tag: T) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_touch(tag).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().alter(tag, s).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn wait_alter<T>(&self, tag: T, s: T::Value) -> Result<(), StateChangeError<T, Self::K>>
    where
        T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
        T::Value: 'static + AsState + Send + Sync,
    {
        self.state_machine().wait_alter(tag, s).await
    }

    #[instrument(level = "trace", skip(self, f))]
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

    #[instrument(level = "trace", skip(self, f))]
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
    watch_impl!(11);
    watch_impl!(12);
    watch_impl!(13);
    watch_impl!(14);
    watch_impl!(15);
    watch_impl!(16);
    watch_impl!(17);
    watch_impl!(18);
    watch_impl!(19);
    watch_impl!(20);

    merge_reader_impl!(2);
    merge_reader_impl!(3);
    merge_reader_impl!(4);
    merge_reader_impl!(5);
    merge_reader_impl!(6);
    merge_reader_impl!(7);
    merge_reader_impl!(8);
    merge_reader_impl!(9);
    merge_reader_impl!(10);
    merge_reader_impl!(11);
    merge_reader_impl!(12);
    merge_reader_impl!(13);
    merge_reader_impl!(14);
    merge_reader_impl!(15);
    merge_reader_impl!(16);
    merge_reader_impl!(17);
    merge_reader_impl!(18);
    merge_reader_impl!(19);
    merge_reader_impl!(20);
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
