use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, futures::Notified};

pub trait AsPassCheck: Debug {
    fn is_open(&self) -> bool;

    fn notified(&self) -> Notified<'_>;
}

#[derive(Debug, Default)]
pub struct Barrier(Arc<AtomicUsize>, Arc<Notify>);

impl Drop for Barrier {
    fn drop(&mut self) {
        let counter = self.0.update(Ordering::Release, Ordering::Acquire, |c| {
            if c == usize::MIN { c } else { c - 1 }
        });
        if counter <= 1 {
            self.1.notify_waiters();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Barriers(Arc<AtomicUsize>, Arc<Notify>);

impl AsPassCheck for Barriers {
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }

    fn notified(&self) -> Notified<'_> {
        self.1.notified()
    }
}

impl Barriers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_barrier(&self) -> Option<Barrier> {
        let counter = self.0.update(Ordering::Release, Ordering::Acquire, |c| {
            if c < usize::MAX { c + 1 } else { c }
        });
        if counter == usize::MAX {
            return None;
        } else {
            Some(Barrier(self.0.clone(), self.1.clone()))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Door(Arc<AtomicBool>, Arc<Notify>);

impl AsPassCheck for Door {
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == false
    }

    fn notified(&self) -> Notified<'_> {
        self.1.notified()
    }
}

impl Door {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&self) {
        self.0.store(false, Ordering::Release);
        self.1.notify_waiters();
    }

    pub fn close(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_door() {
        let res: Arc<AtomicUsize> = Default::default();
        let door = Door::new();
        let func = |join_set: &mut JoinSet<_>, (door, res): (Door, Arc<AtomicUsize>)| {
            join_set.spawn(async move {
                if door.is_open() {
                    println!("door is open");
                    res.fetch_add(1, Ordering::AcqRel);
                } else {
                    println!("door is closed");
                    door.notified().await;
                    println!("door is open now");
                    res.fetch_add(10, Ordering::AcqRel);
                }
            })
        };
        let mut join_set = JoinSet::new();
        for _ in 0..10 {
            let vars = (door.clone(), res.clone());
            func(&mut join_set, vars);
        }
        join_set.join_all().await;
        assert_eq!(10, res.load(Ordering::Acquire));

        door.close();
        res.store(0, Ordering::Release);
        let mut join_set = JoinSet::new();
        for _ in 0..10 {
            let vars = (door.clone(), res.clone());
            func(&mut join_set, vars);
        }
        let door_c = door.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            door_c.open();
        });
        join_set.join_all().await;
        assert_eq!(100, res.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_barrier() {
        let res: Arc<AtomicUsize> = Default::default();
        let barriers = Barriers::new();
        let func = |join_set: &mut JoinSet<_>, (barriers, res): (Barriers, Arc<AtomicUsize>)| {
            join_set.spawn(async move {
                if barriers.is_open() {
                    println!("door is open");
                    res.fetch_add(1, Ordering::AcqRel);
                } else {
                    println!("door is closed");
                    barriers.notified().await;
                    println!("door is open now");
                    res.fetch_add(10, Ordering::AcqRel);
                }
            })
        };
        let mut join_set = JoinSet::new();
        for _ in 0..10 {
            let vars = (barriers.clone(), res.clone());
            func(&mut join_set, vars);
        }
        join_set.join_all().await;
        assert_eq!(10, res.load(Ordering::Acquire));

        res.store(0, Ordering::Release);
        let mut join_set = JoinSet::new();
        let barriers_c = barriers.clone();
        tokio::spawn(async move {
            let barrier = barriers_c.add_barrier();
            assert_eq!(true, barrier.is_some());
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        });
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        for _ in 0..10 {
            let vars = (barriers.clone(), res.clone());
            func(&mut join_set, vars);
        }
        join_set.join_all().await;
        assert_eq!(100, res.load(Ordering::Acquire));
    }
}
