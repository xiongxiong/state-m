use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

#[proc_macro_derive(StateTag)]
pub fn state_tag_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input);
    impl_state_tag_macro(&ast)
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
