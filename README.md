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
- Define 'Tag' enum to distinguish different initiators or responders, all initiators must use different tag values, all responders, and all responders do the same, a same tag value can be used by an initiator and a responder in the same state machine.

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Tag {
    A,
    B(usize)
}
```

- Implement 'HasStateMachine' trait for you data structure, whether it's the initiator or responder of state change, maybe you should add some fields to your data structure.

```rust
#[derive(Debug, Default)]
struct Unit {
    ...
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
```

- If your data structure is also a responder of some state change, implement 'HasStateTarget' trait for your data structure. Then subscribe state sources as needed. Unsubscription is optional, after your state machine is dropped, subscriptions are auto cleaned.

```rust
#[async_trait]
impl HasStateTarget<S, T, Tag> for Unit {
    async fn on_change(
        self: Arc<Self>,
        tag: Tag,
        new_value: T,
        old_value: Option<T>,
    ) -> anyhow::Result<()> {
        ...
    }
```

```rust
let handle_x = unit
        .clone()
        .subscribe(source.reader(), Tag::X)
        .await;
let handle_y = unit
        .clone()
        .convert_subscribe(source.reader(), Tag::Y, |t| {
            Box::pin(async move { format!("X said: Hi, {}", t) })
        })
        .await;
handle_x.unsubscribe();
handle_y.unsubscribe();
```

- Add state change initiators to your state machine, after added, you can get it from state machine by tag. Then change state as needed.

```rust
// add state source to state machine
unit.new_source::<String>(Tag::Hi, Source::new()).await;
// get state source by tag
let source = unit_source.source(TagA::Hi).await;
// change state by need
source.change("Wang".into()).await?;
source.wait_change("Wang".into()).await?;
source.modify(|s| format!("Dear {}", s)).await?;
source.wait_modify(|s| format!("Dear {}", s)).await?;
source.touch().await?;
```

## Example
```rust
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
impl HasStateTarget<String, String, TagB> for UnitB {
    async fn on_change(
        self: Arc<Self>,
        tag: TagB,
        new_value: String,
        old_value: Option<String>,
    ) -> anyhow::Result<()> {
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
async fn test_change() -> anyhow::Result<()> {
    let unit_source = Arc::new(UnitA::default());
    unit_source
        .new_source::<String>(TagA::Hi, Source::new())
        .await;
    let source = unit_source.source(TagA::Hi).await;
    let unit_target = Arc::new(UnitB::default());
    let handle_x = unit_target
        .clone()
        .convert_subscribe(source.reader(), TagB::X, |t| {
            Box::pin(async move { format!("X said: Hi, {}", t) })
        })
        .await;
    let handle_y = unit_target
        .clone()
        .convert_subscribe(source.reader(), TagB::Y, |t| {
            Box::pin(async move { format!("Y said: Hi, {}", t) })
        })
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
```
