use state_m::state_tag;

#[state_tag]
#[kv_assoc(assoc = u8)]
pub struct MyStruct;

#[state_tag]
pub enum MyEnum {
    #[kv_assoc(assoc = ())]
    A,
    #[kv_assoc(assoc = u32)]
    B1(u32),
    #[kv_assoc(assoc = (u32, String))]
    B2(u32, String),
    #[kv_assoc(assoc = String)]
    C1 { name: String },
    #[kv_assoc(assoc = (String, u32))]
    C2 { name: String, age: u32 },
}
