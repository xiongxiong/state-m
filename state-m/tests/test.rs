use state_m::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String)]
    Inner(usize),
    #[kv_assoc(assoc = String)]
    Outer,
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit = Unit::default();
        unit.add_source(TagInner(0), |new, old| {
            tracing::info!("new -- {}, old -- {}", new, old);
            Box::pin(async move { Ok::<_, anyhow::Error>(()) })
        })
        .await?;
        for i in 0..10 {
            unit.alter(TagInner(0), format!("{i}")).await?;
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        Ok(())
    }
}
