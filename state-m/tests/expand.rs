use state_m_macro::*;
use std::{fmt::Debug, hash::Hash};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
pub enum Tag<A, B>
where
    A: Clone + Debug + Eq + Hash + PartialEq + PartialOrd + Ord,
    B: Clone + Debug + Eq + Hash + PartialEq + PartialOrd + Ord,
{
    #[state_tag(assoc = A, label = format!("hello_{:?}", self.0))]
    Hello(A),
    #[state_tag(assoc = B, label = format!("world_{:?}", self.0))]
    World(B),
}
