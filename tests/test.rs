use async_trait::async_trait;
use state_m::*;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Tag {
    A,
}

#[derive(Debug, Default)]
struct Unit {
    lock: Mutex<()>,
    state_machine: StateMachine<Tag>,
}

#[async_trait]
impl HasStateMachine<Tag> for Unit {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    async fn state_machine(&self) -> StateMachine<Tag> {
        self.state_machine.clone()
    }
}

#[async_trait]
impl HasStateTarget<String, String, Tag> for Unit {
    async fn on_change(
        self: Arc<Self>,
        tag: Tag,
        new_value: String,
        old_value: Option<String>,
    ) -> anyhow::Result<()> {
        println!(
            "tag -- {:?}, new_value -- {:?}, old_value -- {:?}",
            tag, new_value, old_value
        );
        Ok(())
    }
}

#[tokio::test]
async fn test() -> anyhow::Result<()> {
    let unit = Arc::new(Unit::default());
    unit.new_source::<String>(Tag::A, Source::new()).await;
    let source_a = unit.source(Tag::A).await;
    let handle = unit.clone().subscribe(source_a.reader(), Tag::A).await;
    source_a.wait_change("Hello".into()).await?;
    handle.unsubscribe();
    Ok(())
}
