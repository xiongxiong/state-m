use async_trait::async_trait;
use state_m::*;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TagA {
    Hi,
}

#[derive(Debug, Default)]
struct UnitA {
    lock: Mutex<()>,
    state_machine: StateMachine<TagA>,
}

#[async_trait]
impl HasStateMachine<TagA> for UnitA {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    async fn state_machine(&self) -> StateMachine<TagA> {
        self.state_machine.clone()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TagB {
    X,
    Y,
}

#[derive(Debug, Default)]
struct UnitB {
    lock: Mutex<()>,
    state_machine: StateMachine<TagB>,
}

#[async_trait]
impl HasStateMachine<TagB> for UnitB {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    async fn state_machine(&self) -> StateMachine<TagB> {
        self.state_machine.clone()
    }
}

#[async_trait]
impl HasStateHandle<String, TagB> for UnitB {
    async fn on_change(
        self: Arc<Self>,
        tag: TagB,
        new_value: String,
        old_value: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match tag {
            TagB::X => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            _ => {}
        }
        println!(
            "tag -- {:?}, new_value -- {:?}, old_value -- {:?}",
            tag, new_value, old_value
        );
        Ok(())
    }
}

#[tokio::test]
async fn test() -> Result<(), SourceChangeError> {
    let unit_source = Arc::new(UnitA::default());
    unit_source.add_source::<String>(TagA::Hi).await;
    let unit_target = Arc::new(UnitB::default());
    unit_target
        .clone()
        .subscribe(unit_source.reader(TagA::Hi).await, TagB::X)
        .await;
    unit_target
        .clone()
        .subscribe::<String>(
            unit_source
                .reader_ex(TagA::Hi, |s| Box::pin(async move { format!("Hi, {}", s) }))
                .await,
            TagB::Y,
        )
        .await;
    unit_source
        .change::<String>(TagA::Hi, "Wang".into())
        .await?;
    unit_source.touch::<String>(TagA::Hi).await?;
    unit_source
        .modify(TagA::Hi, |s| format!("Dear {}", s))
        .await?;
    unit_source
        .wait_change::<String>(TagA::Hi, "Zhang".into())
        .await?;
    unit_source
        .wait_modify(TagA::Hi, |s| format!("Dear {}", s))
        .await?;
    unit_target.unsubscribe::<String>(TagB::X).await;
    unit_target.unsubscribe::<String>(TagB::Y).await;
    unit_source.del_source(TagA::Hi).await;
    Ok(())
}
