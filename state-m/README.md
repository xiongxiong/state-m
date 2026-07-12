# state-m
---
## Summary
The library implements convenient state distribution and management mechanisms, facilitating collaborative work between components.

## Features
* **Separation of read-write**, sources and readers of state changes hold different data structures.
* **Duplicate filtering**, by default, duplicate states do not trigger state changes.
* **State transition**, supports type conversion of state.
* **Timing control**, supports waiting for all readers to complete their work.

## Usage
- Define 'Tag' enum to distinguish different state handles(sources and readers), all handles must use different tag values.
- Derive traits necessary: Clone, Debug, PartialEq, Eq, Hash.
- Use 'state_tag' attribute macro to decorate the 'Tag' enum.
- Add 'kv_assoc' attribute to all variants of the 'Tag' enum, use 'assoc' to associate corresponding state type.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String)]
    Inner(usize),
    #[kv_assoc(assoc = String)]
    Outer,
    #[kv_assoc(assoc = MyState)]
    OuterEx1,
    #[kv_assoc(assoc = usize)]
    OuterEx2,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MyState(usize);

impl From<String> for MyState {
    fn from(value: String) -> Self {
        Self(value.len())
    }
}
```

- Implement 'HasStateMachine' trait for you data structure.

```rust
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
```

- Add state sources to your state machine.

```rust
let unit = Unit::default();
unit.add_source(TagInner(0), 10, |new, old| {
    tracing::info!("new -- {}, old -- {}", new, old);
    Box::pin(async move { Ok::<_, anyhow::Error>(()) })
})
.await?;
```

- Add state readers to your state machine, to respond state changes from outer.
- If the origin state type is not what you need, you can extend the reader to convert the state type as you want.
- The state type must implements these traits: Clone, Debug, Default, PartialEq.

```rust
let unit_b = Unit::default();
unit_b
    .add_reader(TagOuter, unit_a.reader(TagInner(0))?, |new, old| {
        tracing::info!("[unit_b] | new -- {}, old -- {}", new, old);
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            Ok::<_, anyhow::Error>(())
        })
    })
    .await?;
unit_b
    .add_reader(
        TagOuterEx1,
        unit_a.reader(TagInner(0))?.extend(10),
        |new, old| {
            tracing::info!("[unit_b] | new -- {:?}, old -- {:?}", new, old);
            Box::pin(async move { Ok::<_, anyhow::Error>(()) })
        },
    )
    .await?;
unit_b
    .add_reader(
        TagOuterEx2,
        unit_a
            .reader(TagInner(0))?
            .extend_with(10, |s| Box::pin(async move { s.len() })),
        |new, old| {
            tracing::info!("[unit_b] | new -- {}, old -- {}", new, old);
            Box::pin(async move { Ok::<_, anyhow::Error>(()) })
        },
    )
    .await?;
```

- Source state changes as you want, use 'wait_' version of methods if you want to wait for all the responders to finish the work.

```rust
for i in 0..10 {
    unit.alter(TagInner(0), format!("{i}")).await?;
    unit.wait_alter(TagInner(0), format!("[{i}]")).await?;
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
}
for i in 0..10 {
    unit.amend(TagInner(0), |v| format!("{v}_{}", i)).await?;
    unit.wait_amend(TagInner(0), |v| format!("{v}_[{}]", i)).await?;
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
}
unit.touch(TagInner(0)).await?;
unit.wait_touch(TagInner(0)).await?;
```

- Remove state handle as needed

```rust
unit_b.del_handle(&TagOuter);
```
