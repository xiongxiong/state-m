use state_m_macro::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, StateTag)]
pub enum Tag<Id> {
    #[state_tag(assoc = String, label = format!("inner_{:?}", self.0))]
    Hello(Id),
}
