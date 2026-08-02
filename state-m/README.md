# state-m
---
## Summary
The library implements convenient state distribution and management mechanisms, facilitating collaborative work between components.

## Features
* **Separation of concerns**, sources and readers of state changes hold different data structures.
* **Duplicate filtering**, by default, duplicate states do not trigger state changes, you can 'touch' on a tag if you really want this.
* **State transition**, supports type conversion of state.
* **Stop the world (STW)**, supports stopping and resuming processing state change events at state-level.
* **Merge events**, you can merge several state readers into one.
* **Split event**, you can split one state reader into several.
* **Timing control**, supports waiting for all readers to complete their work.
* **Timestamp**, state change event have timestamp, you can know when it changed.

## Usage
- Define 'Tag' enum to distinguish different state handles(sources and readers), all handles must use different tag values.
- Derive traits necessary: Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord.
- Use 'state_tag' attribute macro to decorate the 'Tag' enum.
- Add 'kv_assoc' attribute to all variants of the 'Tag' enum, use 'assoc' (mandatory) to associate corresponding state type, use 'label' (optional) if you want human readable labels when debuging the state machine.
- To be state, a type should implements these traits: Clone, Debug, Default, Display, PartialEq.

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String, label = format!("Layer_{}", self.0))]
    Inner(usize),
    #[kv_assoc(assoc = String, label = "from outer")]
    Outer,
    #[kv_assoc(assoc = CustomType)]
    OuterEx1,
    #[kv_assoc(assoc = usize)]
    OuterEx2,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CustomType(usize);

impl Display for CustomType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomeType({})", self.0)
    }
}

impl From<String> for CustomType {
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
- Add door or barriers to support stopping the world.

```rust
let unit = Unit::default();

// Not support stw.
unit.add_source(TagInner(0), 10, None).await?;

// Support stw by using a Door.
let door = Arc::new(Door::new());
unit
    .add_source(TagInner(0), 10, Some(vec![Box::new(door.clone())]))
    .await?;
// Close or open the door at your need.
door.close();
door.open();

// Support stw by using Barriers.
let barriers = Arc::new(Barriers::new());
unit
    .add_source(TagInner(0), 10, Some(vec![Box::new(barriers.clone())]))
    .await?;
// Add barriers, when the counter of Barriers larger than zero, the state processing stopped, 
// when a barrier is dropped, the counter of Barriers subtract one, when the counter
// reaches zero, the state processing resumed. 
let _barrier_1 = barriers.add_barrier();
let _barrier_2 = barriers.add_barrier();
```

- Add state readers to your state machine, to respond state changes from outer.
- If the origin state type is not what you need, you can extend the reader to convert the state type as you want.
- The state type must implements these traits: Clone, Debug, Default, PartialEq.

```rust
let unit_b = Unit::default();
unit_b.add_reader(TagOuter, unit_a.reader(TagInner(0))?).await?;
unit_b
    .add_reader(
        TagOuterEx1,
        unit_a.reader(TagInner(0))?.derive())
    .await?;
unit_b
    .add_reader(
        TagOuterEx2,
        unit_a
            .reader(TagInner(0))?
            .derive_by(|s| Box::pin(async move { s.len() })),)
    .await?;
```

- Source state changes as you want, use 'wait_' version of methods if you want to wait for all the responders to finish the work.

```rust
for i in 0..10 {
    unit.alter(TagInner(0), format!("{i}")).await?;
    unit.wait_alter(TagInner(0), format!("[{i}]")).await?;
}
for i in 0..10 {
    unit.amend(TagInner(0), |v| format!("{v}_{}", i)).await?;
    unit.wait_amend(TagInner(0), |v| format!("{v}_[{}]", i)).await?;
}
unit.touch(TagInner(0)).await?;
unit.wait_touch(TagInner(0)).await?;
```

- Remove state handle as needed

```rust
unit_b.del_handle(&TagOuter);
```

- Watch state changes from several handles (source or reader), process them in queue without lock.

```rust
unit_b
    .watch(TagOuter(0), move |_, _| {
        let counter_cc = counter_c.clone();
        Box::pin(async move {
            counter_cc.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    })
    .await?;
unit_b
    .watch_2(TagOuter(0), TagOuter(1), |sc_0, sc_1, tag| {
        Box::pin(async move {
            tracing::info!("sc_0 -- {sc_0:?}, sc_1 -- {sc_1:?}, tag -- {tag:?}");
            anyhow::Ok(())
        })
    })
    .await?;
```

- Merge several readers into one.

```rust
let reader_2 = unit_b
    .merge_reader_2(TagOuter(0), TagOuter(1), |a, b| State {
        value: format!("merged [{}] and [{}]", a.value, b.value),
        timestamp: Utc::now(),
    })
    .await?;
```

- Split one reader into several readers.

```rust
let (reader_1, reader_2) = unit_b
    .split_reader_2(TagOuter(0), |ref v| (format!("NEW_{}", v), v.len()))
    .await?;
```

- Debug the state machine as required.

```rust
tracing::info!("state_machine: unit_b\n{:?}", unit_b.state_machine);
```

```bash
2026-07-17T14:21:52.899915Z  INFO test::tests: state_machine: unit_b
from outer           | 2026-07-17 14:21:52.899773965 UTC | "A_9"
TagOuterEx2          | 2026-07-17 14:21:52.898533334 UTC | 3
from outer           | 2026-07-17 14:21:52.899773965 UTC | "NEW_A_9"
```
