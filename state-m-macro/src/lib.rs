use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn state_tag(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item);
    todo!()
}

fn impl_state_tag_macro(ast: &syn::DeriveInput) -> TokenStream {
    let tag_name = &ast.ident;
    let mut quotes: Vec<TokenStream> = Vec::new();
    match &ast.data {
        syn::Data::Struct(data_struct) => todo!(),
        syn::Data::Enum(data_enum) => todo!(),
        syn::Data::Union(data_union) => todo!(),
    }
    let generated = quote! {};
    generated.into()
}
