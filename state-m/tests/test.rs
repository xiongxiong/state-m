use state_m::*;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String, label = format!("inner_{}", self.0))]
    Inner(usize),
    #[kv_assoc(assoc = String, label = "from outer")]
    Outer(usize),
    #[kv_assoc(assoc = CustomType)]
    OuterEx1,
    #[kv_assoc(assoc = usize)]
    OuterEx2,
}

#[derive(Clone, Debug, Default)]
pub struct Unit {
    state_machine: StateMachine<Tag>,
}

impl HasStateMachine for Unit {
    type K = Tag;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CustomType(usize);

impl Display for CustomType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomeType({})", self.0)
    }
}

impl From<String> for CustomType {
    fn from(value: String) -> Self {
        Self(value.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::sync::{
        Arc, Once,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::task::JoinSet;

    static CALL_ONCE: Once = Once::new();

    fn init_tracing() {
        CALL_ONCE.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .init();
        });
    }

    #[tokio::test]
    async fn test_debug_state() -> Result<()> {
        let sm: Arc<StateMachine<Tag>> = Default::default();
        assert_eq!("", &format!("{sm:?}"));
        Ok(())
    }

    #[tokio::test]
    async fn test_normal() -> Result<()> {
        init_tracing();
        let unit = Unit::default();
        unit.add_source(TagInner(0), 10, None).await?;
        for i in 0..10 {
            unit.alter(TagInner(0), format!("{i}")).await?;
        }
        for i in 0..10 {
            unit.amend(TagInner(0), |v| format!("{v}_{}", i)).await?;
        }
        unit.wait_touch(TagInner(0)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_wait() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_a.wait_alter(TagInner(0), "A".into()).await?;
        unit_a.wait_alter(TagInner(0), "B".into()).await?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_extend() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuterEx1, unit_a.reader(TagInner(0))?.derive())
            .await?;
        unit_b
            .add_reader(
                TagOuterEx2,
                unit_a.reader(TagInner(0))?.derive_by(|s| s.len()),
            )
            .await?;
        unit_a.alter(TagInner(0), "Hello".into()).await?;
        unit_a.alter(TagInner(0), "Workspace".into()).await?;
        unit_a.wait_alter(TagInner(0), "Love".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_a.wait_alter(TagInner(0), "A".into()).await?;
        unit_a.wait_alter(TagInner(0), "B".into()).await?;
        unit_b.del_handle(&TagOuter(0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_watch() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_b
            .add_reader(TagOuter(1), unit_a.reader(TagInner(1))?)
            .await?;
        unit_b
            .watch_2(TagOuter(0), TagOuter(1), |sc_0, sc_1, tag| {
                Box::pin(async move {
                    tracing::info!("sc_0 -- {sc_0:?}, sc_1 -- {sc_1:?}, tag -- {tag:?}");
                    anyhow::Ok(())
                })
            })
            .await?;
        for i in 0..10 {
            unit_a.alter(TagInner(0), format!("A_{i}")).await?;
            unit_a.alter(TagInner(1), format!("B_{i}")).await?;
        }
        tracing::info!("state_machine: unit_a\n{:?}", unit_a.state_machine);
        tracing::info!("state_machine: unit_b\n{:?}", unit_b.state_machine);
        unit_b.del_handle(&TagOuter(0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_b
            .add_reader(TagOuter(1), unit_a.reader(TagInner(1))?)
            .await?;
        let reader_2 = unit_b
            .merge_reader_2(TagOuter(0), TagOuter(1), |a, b| {
                format!("merged [{}] and [{}]", a, b)
            })
            .await?;
        unit_b.add_reader(TagOuter(2), reader_2).await?;
        for i in 0..10 {
            unit_a.alter(TagInner(0), format!("A_{i}")).await?;
            unit_a.alter(TagInner(1), format!("B_{i}")).await?;
        }
        tracing::info!("state_machine: unit_a\n{:?}", unit_a.state_machine);
        tracing::info!("state_machine: unit_b\n{:?}", unit_b.state_machine);
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_split() -> Result<()> {
        init_tracing();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        let (reader_1, reader_2) = unit_b
            .split_reader_2(TagOuter(0), |ref v| (format!("NEW_{}", v), v.len()))
            .await?;
        unit_b.add_reader(TagOuter(1), reader_1).await?;
        unit_b.add_reader(TagOuterEx2, reader_2).await?;
        for i in 0..10 {
            unit_a.alter(TagInner(0), format!("A_{i}")).await?;
        }
        tracing::info!("state_machine: unit_a\n{:?}", unit_a.state_machine);
        tracing::info!("state_machine: unit_b\n{:?}", unit_b.state_machine);
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stw_by_door() -> Result<()> {
        init_tracing();
        const COUNT: usize = 1000;
        let door = Arc::new(Door::new());
        let unit_a = Unit::default();
        unit_a
            .add_source(TagInner(0), 10, Some(vec![Box::new(door.clone())]))
            .await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_c = counter.clone();
        unit_b
            .watch(TagOuter(0), move |_, _| {
                let counter_cc = counter_c.clone();
                Box::pin(async move {
                    counter_cc.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
            })
            .await?;
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            let mut i = 0;
            while i < COUNT {
                match unit_a.alter(TagInner(0), format!("A_{i}")).await {
                    Ok(_) => {
                        i += 1;
                    }
                    Err(e) => tracing::error!("{e}"),
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            anyhow::Ok(())
        });
        join_set.spawn(async move {
            for _ in 0..100 {
                door.close();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                door.open();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok(())
        });
        join_set.join_all().await;
        assert_eq!(COUNT, counter.load(Ordering::Acquire));
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_stw_by_barrier() -> Result<()> {
        init_tracing();
        const COUNT: usize = 1000;
        let barriers = Arc::new(Barriers::new());
        let unit_a = Unit::default();
        unit_a
            .add_source(TagInner(0), 10, Some(vec![Box::new(barriers.clone())]))
            .await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_c = counter.clone();
        unit_b
            .watch(TagOuter(0), move |_, _| {
                let counter_cc = counter_c.clone();
                Box::pin(async move {
                    counter_cc.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
            })
            .await?;
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            let mut i = 0;
            while i < COUNT {
                match unit_a.alter(TagInner(0), format!("A_{i}")).await {
                    Ok(_) => {
                        i += 1;
                    }
                    Err(e) => tracing::error!("{e}"),
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            anyhow::Ok(())
        });
        join_set.spawn(async move {
            for _ in 0..100 {
                let _barrier_1 = barriers.add_barrier();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let _barrier_2 = barriers.add_barrier();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let _barrier_3 = barriers.add_barrier();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok(())
        });
        join_set.join_all().await;
        assert_eq!(COUNT, counter.load(Ordering::Acquire));
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }
}
