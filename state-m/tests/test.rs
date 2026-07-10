use state_m::{KvAssoc, state_tag};

#[state_tag]
#[kv_assoc(assoc = u8)]
pub struct MyStruct;

#[state_tag]
pub enum MyEnum {
    #[kv_assoc(assoc = String)]
    A,
    #[kv_assoc(assoc = u8)]
    B(u32),
    #[kv_assoc(assoc = u8)]
    C { name: String },
}

pub struct Hello(MyEnumA);

impl Hello {
    fn tt(&self) {
        let x: <MyEnumA as KvAssoc>::Value = "".into();
    }
}
