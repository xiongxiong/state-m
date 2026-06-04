pub struct Source {}

pub struct Machine {}

pub enum Event {
    Noti,
}

pub trait AsStateMachine {}

pub trait AsSubscriber<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
