// use state_m::*;
// use state_m_macro::*;
// use std::{fmt::Debug, hash::Hash, marker::PhantomData};

// #[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, StateTag)]
// #[state_tag(assoc = String, label = format!("my_id_{}", self.0))]
// pub struct MyId(u8);

// watch_group_sm!(2);

// #[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
// pub enum Tag<A, B>
// where
//     A: AsKey + KvAssoc,
//     B: AsKey + KvAssoc,
// {
//     #[state_tag(generics = <A>, assoc = A::Value, label = format!("unit"))]
//     Unit(PhantomData<A>),
//     #[state_tag(assoc = A::Value, label = format!("hello_{:?}", self.0))]
//     Hello(A),
//     #[state_tag(assoc = B::Value, label = format!("world_{:?}", self.0))]
//     World(B),
// }
