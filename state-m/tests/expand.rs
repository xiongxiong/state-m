use state_m_macro::*;
use std::{fmt::Debug, hash::Hash};

// #[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
// pub enum Tag<Id>
// where
//     Id: Clone + Debug + Eq + Hash + PartialEq + PartialOrd + Ord,
// {
//     #[state_tag(assoc = Id, label = format!("inner_{:?}", self.0))]
//     Hello(Id),
// }
