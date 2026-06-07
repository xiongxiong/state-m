use async_trait::async_trait;
use state_m::*;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Tag {
    Hi,
    A,
    B,
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
        match tag {
            Tag::A => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Tag::B => {
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
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
async fn test_change() -> anyhow::Result<()> {
    let unit = Arc::new(Unit::default());
    unit.new_source::<String>(Tag::Hi, Source::new()).await;
    let source = unit.source(Tag::Hi).await;
    let handle_a = unit
        .clone()
        .convert_subscribe(source.reader(), Tag::A, |t| {
            Box::pin(async move { format!("A said: Hi {}", t) })
        })
        .await;
    let handle_b = unit
        .clone()
        .convert_subscribe(source.reader(), Tag::B, |t| {
            Box::pin(async move { format!("B said: Hi {}", t) })
        })
        .await;
    source.change("Wang".into()).await?;
    source.change("Li".into()).await?;
    source.wait_change("Zhang".into()).await?;
    handle_a.unsubscribe();
    handle_b.unsubscribe();
    Ok(())
}
