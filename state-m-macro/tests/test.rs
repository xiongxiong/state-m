use state_m_macro::state_tag;

pub struct MyStruct;

// #[state_tag]
pub enum MyEnum {
    #[state_tag(assoc = String)]
    A,
    #[state_tag(assoc = u8)]
    B,
}
