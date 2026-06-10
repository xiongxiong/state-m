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
impl HasLock for UnitA {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }
}

#[async_trait]
impl HasStateMachine<TagA> for UnitA {
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
impl HasLock for UnitB {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }
}

#[async_trait]
impl HasStateMachine<TagB> for UnitB {
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
        old_value: Option<String>,
    ) -> Result<(), std::fmt::Error> {
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
    unit_source
        .add_source::<String>(TagA::Hi, Source::new())
        .await;
    let source = unit_source.source(TagA::Hi).await;
    let unit_target = Arc::new(UnitB::default());
    let handle_x = unit_target
        .clone()
        .subscribe(source.reader(), TagB::X)
        .await;
    let handle_y = unit_target
        .clone()
        .subscribe(
            source.reader_ex(Arc::new(|s| Box::pin(async move { format!("Hi, {}", s) }))),
            TagB::Y,
        )
        .await;
    source.change("Wang".into()).await?;
    source.touch().await?;
    source.modify(|s| format!("Dear {}", s)).await?;
    source.wait_change("Zhang".into()).await?;
    source.wait_modify(|s| format!("Dear {}", s)).await?;
    handle_x.unsubscribe();
    handle_y.unsubscribe();
    Ok(())
}
