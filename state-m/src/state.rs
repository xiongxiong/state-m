use chrono::{DateTime, Utc};
use crossfire::{mpmc::Null, null::CloseHandle};
use derivative::Derivative;
use std::fmt::Display;

#[derive(Clone, Debug)]
pub struct State<S>
where
    S: Default,
{
    pub value: S,
    pub timestamp: DateTime<Utc>,
}

impl<S> Default for State<S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            value: Default::default(),
            timestamp: Utc::now(),
        }
    }
}

impl<S> Display for State<S>
where
    S: Display + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.value, self.timestamp)
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct StateEvent<S>
where
    S: Default,
{
    pub state: State<S>,
    pub is_touch: bool,
    #[derivative(Debug = "ignore")]
    pub close_handle: Option<CloseHandle<Null>>,
}

impl<S> Default for StateEvent<S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            state: Default::default(),
            is_touch: Default::default(),
            close_handle: Default::default(),
        }
    }
}
