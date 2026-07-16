use state_m::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String, label = format!("inner_{}", self.0))]
    Inner(usize),
    #[kv_assoc(assoc = String, label = "Outer")]
    Outer(usize),
    #[kv_assoc(assoc = MyState)]
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
pub struct MyState(usize);

impl From<String> for MyState {
    fn from(value: String) -> Self {
        Self(value.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test_normal() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit = Unit::default();
        unit.add_source(TagInner(0), 10).await?;
        for i in 0..10 {
            unit.alter(TagInner(0), format!("{i}")).await?;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        for i in 0..10 {
            unit.amend(TagInner(0), |v| format!("{v}_{}", i)).await?;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        unit.wait_touch(TagInner(0)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_wait() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10).await?;
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
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuterEx1, unit_a.reader(TagInner(0))?.extend(10))
            .await?;
        unit_b
            .add_reader(
                TagOuterEx2,
                unit_a.reader(TagInner(0))?.extend_with(10, |s| s.len()),
            )
            .await?;
        unit_a.alter(TagInner(0), "Hello".into()).await?;
        unit_a.alter(TagInner(0), "Workspace".into()).await?;
        unit_a.wait_alter(TagInner(0), "Love".into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10).await?;
        let unit_b = Unit::default();
        unit_b
            .add_reader(TagOuter(0), unit_a.reader(TagInner(0))?)
            .await?;
        unit_a.wait_alter(TagInner(0), "A".into()).await?;
        unit_a.wait_alter(TagInner(0), "B".into()).await?;
        unit_a
            .state_machine
            .debug_states()
            .await
            .iter()
            .for_each(|s| println!("{}", s));
        unit_b
            .state_machine
            .debug_states()
            .await
            .iter()
            .for_each(|s| println!("{}", s));
        unit_b.del_handle(&TagOuter(0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        unit_a
            .state_machine
            .debug_states()
            .await
            .iter()
            .for_each(|s| println!("{}", s));
        unit_b
            .state_machine
            .debug_states()
            .await
            .iter()
            .for_each(|s| println!("{}", s));
        Ok(())
    }

    #[tokio::test]
    async fn test_watch() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit_a = Unit::default();
        unit_a.add_source(TagInner(0), 10).await?;
        unit_a.add_source(TagInner(1), 10).await?;
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
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            unit_a.alter(TagInner(1), format!("B_{i}")).await?;
        }
        unit_b.del_handle(&TagOuter(0))?;
        unit_a.wait_alter(TagInner(0), "C".into()).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        unit_a.wait_alter(TagInner(0), "D".into()).await?;
        Ok(())
    }
}
