// use state_m::{AsKey, KvAssoc};
// use state_m_macro::*;
// use std::{fmt::Debug, hash::Hash};

// sm_watch!(1);

// #[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
// pub enum Tag<A, B>
// where
//     A: AsKey + KvAssoc,
//     B: AsKey + KvAssoc,
// {
//     #[state_tag(assoc = A::Value, label = format!("hello_{:?}", self.0))]
//     Hello(A),
//     #[state_tag(assoc = B::Value, label = format!("world_{:?}", self.0))]
//     World(B),
// }
