use std::sync::Arc;

use state_m::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum TagA {
    #[kv_assoc(assoc = String)]
    Inner(usize),
}

#[derive(Clone, Debug, Default)]
pub struct UnitA {
    state_machine: StateMachine<TagA>,
}

impl HasStateMachine for UnitA {
    type K = TagA;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum TagB {
    #[kv_assoc(assoc = String)]
    Outer(usize),
}

#[derive(Clone, Debug, Default)]
pub struct UnitB {
    state_machine: Arc<StateMachine<TagB>>,
}

impl HasStateMachine for UnitB {
    type K = TagB;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test_on_change() -> Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();
        let unit_a = UnitA::default();
        unit_a.add_source(TagAInner(0), 10).await?;
        let unit_b = UnitB::default();
        unit_b
            .add_reader(TagBOuter(0), unit_a.reader(TagAInner(0))?)
            .await?;
        for i in 0..10 {
            unit_a.alter(TagAInner(0), format!("{i}")).await?;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Ok(())
    }
}
