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

- If your data structure is also a responder of some state change, implement 'HasStateHandle' trait for your data structure. Then subscribe state sources as needed. Unsubscription is optional, after your state machine is dropped, subscriptions are auto cleaned.

```rust
#[async_trait]
impl HasStateHandle<S, T, Tag> for Unit {
    async fn on_change(
        self: Arc<Self>,
        tag: Tag,
        new_value: T,
        old_value: T,
    ) -> Result<(), Error> {
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
        .subscribe(
            source.reader_ex(|s| Box::pin(async move { format!("Hi, {}", s) })),
            Tag::Y,
        )
        .await;
handle_x.unsubscribe();
handle_y.unsubscribe();
```

- Add state change initiators to your state machine, after added, you can get it from state machine by tag. Then change state as needed.

```rust
// add state source to state machine
unit.add::<String>(Tag::Hi).await;
unit.add_ex::<String>(Tag::Hi, Source::create("my init value", 100)).await;
// get state source by tag
let source = unit_source.source(TagA::Hi).await;
// change state by need
source.change("Wang".into()).await?;
source.wait_change("Wang".into()).await?;
source.modify(|s| format!("Dear {}", s)).await?;
source.wait_modify(|s| format!("Dear {}", s)).await?;
source.touch().await?;
```
