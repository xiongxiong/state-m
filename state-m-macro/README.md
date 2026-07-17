# state-m-macro
---
## Summary
Macros for state-m. The only exported macro is 'state_tag'.

## Usage
- Define 'Tag' enum to distinguish different state handles(sources and readers), all handles must use different tag values.
- Derive traits necessary: Clone, Debug, PartialEq, Eq, Hash.
- Use 'state_tag' attribute macro to decorate the 'Tag' enum.
- Add 'kv_assoc' attribute to all variants of the 'Tag' enum, use 'assoc' (mandatory) to associate corresponding state type, use 'label' (optional) if you want human readable labels when debuging the state machine.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum Tag {
    #[kv_assoc(assoc = String, label = format!("Layer_{}", self.0))]
    Inner(usize),
    #[kv_assoc(assoc = String, label = "from outer")]
    Outer,
    #[kv_assoc(assoc = MyState)]
    OuterEx1,
    #[kv_assoc(assoc = usize)]
    OuterEx2,
}
