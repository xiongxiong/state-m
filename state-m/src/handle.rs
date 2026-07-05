use std::fmt::Debug;

use crate::{reader::Reader, source::Source};

pub enum Handle<S>
where
    S: 'static + Clone + Debug + Default + PartialEq,
{
    Source(Source<S>),
    Reader(Reader<S>),
}
