use state_m::*;
use std::{fmt::Debug, sync::Arc};

#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, StateTag)]
#[state_tag(assoc = String, label = format!("my_id_{}", self.0))]
pub struct TagS(u8);

#[derive(Clone, Debug, Default)]
pub struct UnitS {
    state_machine: StateMachine<TagS>,
}

impl HasStateMachine for UnitS {
    type K = TagS;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, StateTag)]
pub enum Tag<IdI, IdO, IdX>
where
    IdI: AsKey + KvAssoc,
    IdO: AsKey + KvAssoc,
    IdX: AsState,
{
    #[state_tag(assoc = String, label = format!("inner_{}", self.0))]
    Inner(usize),
    #[state_tag(assoc = String, label = format!("outer_{}_{}", self.0, self.1))]
    Outer(usize, usize),
    #[state_tag(assoc = MyCusTyp, hidden = true)]
    OuterEx1,
    #[state_tag(assoc = usize)]
    OuterEx2(OuterEx2),
    #[state_tag(assoc = OuterEx3)]
    OuterEx3,
    #[state_tag(assoc = IdI::Value, label = format!("route_i_{:?}", self.0))]
    RouteI(IdI),
    #[state_tag(assoc = IdO::Value, label = format!("route_o_{:?}", self.0))]
    RouteO(IdO),
    #[state_tag(generics = <IdI, IdO>, assoc = (IdI::Value, IdO::Value) label = format!("uni_route_i"))]
    UniRouteI,
    #[state_tag(generics = <IdI, IdO>, assoc = (IdI::Value, IdO::Value) label = format!("uni_route_o"))]
    UniRouteO,
    #[state_tag(phantom = true)]
    Phantom(IdX),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MyCusTyp(usize);

impl From<String> for MyCusTyp {
    fn from(value: String) -> Self {
        Self(value.len())
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct OuterEx2 {
    pub name: String,
    pub age: usize,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct OuterEx3 {
    pub name: String,
    pub age: usize,
    pub weight: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Unit<IdI, IdO>
where
    IdI: AsKey + KvAssoc,
    IdO: AsKey + KvAssoc,
{
    state_machine: StateMachine<Tag<IdI, IdO, ()>>,
}

impl<IdI, IdO> Unit<IdI, IdO>
where
    IdI: AsKey + KvAssoc,
    IdO: AsKey + KvAssoc,
{
    pub fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            state_machine: StateMachine::<Tag<IdI, IdO, ()>>::new(id),
        })
    }
}

impl<IdI, IdO> HasStateMachine for Unit<IdI, IdO>
where
    IdI: AsKey + KvAssoc,
    IdO: AsKey + KvAssoc,
{
    type K = Tag<IdI, IdO, ()>;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
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
        let sm: Arc<StateMachine<Tag<TagS, TagS, ()>>> = Default::default();
        assert_eq!("", &format!("{sm:?}"));
        Ok(())
    }

    #[tokio::test]
    async fn test_struct_tag() -> Result<()> {
        init_tracing();
        let unit = UnitS::default();
        unit.add_source(TagS(0), 10, None).await?;
        for i in 0..10 {
            unit.alter(TagS(0), format!("{i}")).await?;
        }
        for i in 0..10 {
            unit.amend(TagS(0), |v| format!("{v}_{}", i)).await?;
        }
        unit.wait_touch(TagS(0)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_alter() -> Result<()> {
        init_tracing();
        let unit = Unit::<TagS, TagS>::new("X");
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
    async fn test_amend() -> Result<()> {
        init_tracing();
        let unit = Unit::<TagS, TagS>::new("X");
        unit.add_source(TagOuterEx3, 10, None).await?;
        let unit_c = unit.clone();
        let h_1 = tokio::spawn(async move {
            for _ in 0..1000 {
                unit_c
                    .amend(TagOuterEx3, |v| OuterEx3 {
                        age: v.age + 1,
                        ..v
                    })
                    .await?;
            }
            anyhow::Ok(())
        });
        let unit_c = unit.clone();
        let h_2 = tokio::spawn(async move {
            for _ in 0..1000 {
                unit_c
                    .amend(TagOuterEx3, |v| OuterEx3 {
                        weight: v.weight + 1,
                        ..v
                    })
                    .await?;
            }
            anyhow::Ok(())
        });
        _ = h_1.await?;
        _ = h_2.await?;
        assert_eq!(1000, unit.value(TagOuterEx3)?.age);
        assert_eq!(1000, unit.value(TagOuterEx3)?.weight);
        Ok(())
    }

    #[tokio::test]
    async fn test_wait() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_a.wait_alter(TagInner(0), "A".into()).await?;
        unit_a.wait_alter(TagInner(0), "B".into()).await?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_extend() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuterEx1, unit_a.reader(TagInner(0))?.derive())
            .await?;
        unit_b
            .add_reader(
                TagOuterEx2(OuterEx2 {
                    name: "wang".into(),
                    age: 18,
                }),
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
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_a.wait_alter(TagInner(0), "A".into()).await?;
        unit_a.wait_alter(TagInner(0), "B".into()).await?;
        unit_b.del_handle(&TagOuter(0, 0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_watch() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_b
            .add_reader(TagOuter(0, 1), unit_a.reader(TagInner(1))?)
            .await?;
        unit_b
            .watch_2(TagOuter(0, 0), TagOuter(0, 1), |sc_0, sc_1| {
                Box::pin(async move {
                    tracing::info!("sc_0 -- {sc_0:?}, sc_1 -- {sc_1:?}");
                    return Err(anyhow::anyhow!("error happened"));
                })
            })
            .await?;
        for i in 0..10 {
            unit_a.alter(TagInner(0), format!("A_{i}")).await?;
            unit_a.alter(TagInner(1), format!("B_{i}")).await?;
        }
        tracing::info!("state_machine: unit_a\n{:?}", unit_a.state_machine);
        tracing::info!("state_machine: unit_b\n{:?}", unit_b.state_machine);
        unit_b.del_handle(&TagOuter(0, 0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_reader() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        let reader_0 = unit_a.reader(TagInner(0))?;
        let reader_1 = unit_a.reader(TagInner(1))?;
        let reader_m = influx_reader_2(
            reader_0.derive_by(|s| format!("from_0 | {s}")),
            reader_1.derive_by(|s| format!("from_1 | {s}")),
            10,
        );
        unit_b.add_reader(TagOuter(0, 0), reader_m).await?;
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
    async fn test_merge_group() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        let reader_0 = unit_a.reader(TagInner(0))?;
        let reader_1 = unit_a.reader(TagInner(1))?;
        let reader_m = influx_group(
            vec![
                reader_0.derive_by(|s| format!("from_0 | {s}")),
                reader_1.derive_by(|s| format!("from_1 | {s}")),
            ],
            10,
        );
        unit_b.add_reader(TagOuter(0, 0), reader_m).await?;
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
    async fn test_merge_sm() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_b
            .add_reader(TagOuter(0, 1), unit_a.reader(TagInner(1))?)
            .await?;
        let reader_2 = unit_b
            .merge_reader_2(TagOuter(0, 0), TagOuter(0, 1), 10, |a, b| {
                format!("merged [{}] and [{}]", a, b)
            })
            .await?;
        unit_b.add_reader(TagOuter(0, 2), reader_2).await?;
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
    async fn test_merge_group_sm() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        unit_a.add_source(TagInner(1), 10, None).await?;
        let reader = unit_a
            .merge_group::<TagInner, _, _>(10, |states| {
                states
                    .iter()
                    .fold("".into(), |init, item| format!("{init}, {}", item.1))
            })
            .await?;
        unit_a.add_reader(TagOuter(0, 0), reader).await?;
        for i in 0..10 {
            unit_a.alter(TagInner(0), format!("A_{i}")).await?;
            unit_a.alter(TagInner(1), format!("B_{i}")).await?;
        }
        tracing::info!("state_machine: unit_a\n{:?}", unit_a.state_machine);
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_split() -> Result<()> {
        init_tracing();
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a.add_source(TagInner(0), 10, None).await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        let (reader_1, reader_2) = unit_b
            .split_reader_2(TagOuter(0, 0), |ref v| (format!("NEW_{}", v), v.len()))
            .await?;
        unit_b.add_reader(TagOuter(0, 1), reader_1).await?;
        unit_b
            .add_reader(
                TagOuterEx2(OuterEx2 {
                    name: "wang".into(),
                    age: 18,
                }),
                reader_2,
            )
            .await?;
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
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a
            .add_source(TagInner(0), 10, Some(vec![Arc::new(door.clone())]))
            .await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_c = counter.clone();
        unit_b
            .watch(TagOuter(0, 0), move |_| {
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
        let unit_a = Unit::<TagS, TagS>::new("A");
        unit_a
            .add_source(TagInner(0), 10, Some(vec![Arc::new(barriers.clone())]))
            .await?;
        let unit_b = Unit::<TagS, TagS>::new("B");
        unit_b
            .add_reader(TagOuter(0, 0), unit_a.reader(TagInner(0))?)
            .await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_c = counter.clone();
        unit_b
            .watch(TagOuter(0, 0), move |_| {
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
