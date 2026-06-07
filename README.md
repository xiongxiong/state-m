# state-m
---
<h5>
  The library implements convenient state distribution and management mechanisms, facilitating collaborative work between components.
</h5>

```toml
[dependencies]
state-m = "0.1.0"
```

## Features
* **Separation of read-write**, initiators and responders of state changes hold different data structures.
* **Duplicate filtering**, by default, duplicate states do not trigger state changes.
* **State transition**, supports type conversion of subscription state changes.
* **Timing control**, supports waiting for all responders to complete their responses.

## Usage
```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SourceTag {
    Hi,
}

#[derive(Debug, Default)]
struct UnitSource {
    lock: Mutex<()>,
    state_machine: StateMachine<SourceTag>,
}

#[async_trait]
impl HasStateMachine<SourceTag> for UnitSource {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    async fn state_machine(&self) -> StateMachine<SourceTag> {
        self.state_machine.clone()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TargetTag {
    A,
    B,
}

#[derive(Debug, Default)]
struct UnitTarget {
    lock: Mutex<()>,
    state_machine: StateMachine<TargetTag>,
}

#[async_trait]
impl HasStateMachine<TargetTag> for UnitTarget {
    async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    async fn state_machine(&self) -> StateMachine<TargetTag> {
        self.state_machine.clone()
    }
}

#[async_trait]
impl HasStateTarget<String, String, TargetTag> for UnitTarget {
    async fn on_change(
        self: Arc<Self>,
        tag: TargetTag,
        new_value: String,
        old_value: Option<String>,
    ) -> anyhow::Result<()> {
        match tag {
            TargetTag::B => {
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
async fn test_change() -> anyhow::Result<()> {
    let unit_source = Arc::new(UnitSource::default());
    unit_source
        .new_source::<String>(SourceTag::Hi, Source::new())
        .await;
    let source = unit_source.source(SourceTag::Hi).await;
    let unit_target = Arc::new(UnitTarget::default());
    let handle_a = unit_target
        .clone()
        .convert_subscribe(source.reader(), TargetTag::A, |t| {
            Box::pin(async move { format!("A said: Hi {}", t) })
        })
        .await;
    let handle_b = unit_target
        .clone()
        .convert_subscribe(source.reader(), TargetTag::B, |t| {
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
```
