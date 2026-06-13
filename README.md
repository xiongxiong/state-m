# state-m
---
The library implements convenient state distribution and management mechanisms, facilitating collaborative work between components.

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

- If your data structure is also a responder of some state change, implement 'HasStateHandle' trait for your data structure. Then subscribe sources as needed. Unsubscription is optional, after your state machine is dropped, subscriptions are auto cleaned.

```rust
#[async_trait]
impl HasStateHandle<S, T, Tag> for Unit {
    async fn on_change(
        self: Arc<Self>,
        tag: Tag,
        new_value: T,
        old_value: T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        ...
    }
```

```rust
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
```

- Add state change initiators to your state machine, after added, you can get it from state machine by tag. Then change state as needed.

```rust
// add source to state machine
unit_source.add_source::<String>(TagA::Hi).await;
unit_source.add_source_ex::<String>(TagA::Hi, 100, "Hello").await;
// change state by need
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
```

- Do unsubscrption as needed

```rust
unit_target.unsubscribe::<String>(TagB::X).await;
unit_target.unsubscribe::<String>(TagB::Y).await;
unit_source.del_source(&TagA::Hi).await;
```
