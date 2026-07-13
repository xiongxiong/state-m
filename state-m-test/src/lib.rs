use std::sync::Arc;

use state_m::*;

// #[state_tag]
// #[kv_assoc(assoc = u8)]
// pub struct MyStruct;

// #[state_tag]
// pub enum MyEnum {
//     #[kv_assoc(assoc = ())]
//     A,
//     #[kv_assoc(assoc = u32)]
//     B1(u32),
//     #[kv_assoc(assoc = (u32, String))]
//     B2(u32, String),
//     #[kv_assoc(assoc = String)]
//     C1 { name: String },
//     #[kv_assoc(assoc = (String, u32))]
//     C2 { name: String, age: u32 },
// }

// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
// #[state_tag]
// pub enum TagA {
//     #[kv_assoc(assoc = String)]
//     Inner(usize),
// }

// #[derive(Clone, Debug, Default)]
// pub struct UnitA {
//     state_machine: StateMachine<TagA>,
// }

// impl HasStateMachine for UnitA {
//     type K = TagA;

//     fn state_machine(&self) -> &StateMachine<Self::K> {
//         &self.state_machine
//     }
// }

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[state_tag]
pub enum TagB {
    #[kv_assoc(assoc = String)]
    Outer(usize),
}

#[derive(Clone, Debug, Default)]
pub struct UnitB {
    state_machine: Arc<StateMachine<TagB>>,
}

impl HasStateMachine for UnitB {
    type K = TagB;

    fn state_machine(&self) -> &StateMachine<Self::K> {
        &self.state_machine
    }
}
