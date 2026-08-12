use state_m_macro::*;
use std::{fmt::Debug, hash::Hash};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
pub enum Tag<Id>
where
    Id: Clone + Debug + Eq + Hash + PartialEq + PartialOrd + Ord,
{
    #[state_tag(assoc = Id, label = format!("inner_{:?}", self.0))]
    Hello(Id),
}

// pub struct TagHello<Id>(pub Id);

// impl<Id: ::core::clone::Clone> ::core::clone::Clone for TagHello<Id> {
//     #[inline]
//     fn clone(&self) -> TagHello<Id> {
//         TagHello(::core::clone::Clone::clone(&self.0))
//     }
// }

// impl<Id> From<TagHello<Id>> for Tag<Id> {
//     fn from(value: TagHello<Id>) -> Tag<Id> {
//         Tag::Hello(value.0)
//     }
// }

// impl<Id> state_m::KvAssoc for TagHello<Id>
// where
//     Id: Clone + Debug + Hash + PartialEq + PartialOrd,
// {
//     type Key = Tag<Id>;
//     type Value = String;
// }

// impl<Id> std::fmt::Debug for TagHello<Id>
// where
//     Id: std::fmt::Debug,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         todo!()
//     }
// }
