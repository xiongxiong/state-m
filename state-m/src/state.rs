use chrono::{DateTime, Utc};
use derivative::Derivative;
use std::fmt::Display;
use tokio::sync::mpsc;

/// State type.
/// * `value` - the state value.
/// * `timestamp` - timestamp at which the current state value was created.
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

/// State change event.
/// * `state` - state associated with event.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct StateEvent<S>
where
    S: Default,
{
    pub state: State<S>,
    pub(crate) is_touch: bool,
    #[derivative(Debug = "ignore")]
    pub(crate) close_handle: Option<mpsc::Sender<()>>,
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
