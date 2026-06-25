use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Attribute, DeriveInput, parse_macro_input};

#[proc_macro_attribute]
pub fn state_tag(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let name_prefix = "St";
    let input = parse_macro_input!(item as DeriveInput);
    let tag_name = &input.ident;
    let tag_vis = &input.vis;
    let mut quotes: Vec<TokenStream2> = Vec::new();
    match input.data {
        syn::Data::Struct(_) | syn::Data::Union(_) => {
            let q_name = format_ident!("{}{}", name_prefix, tag_name);
            let q = quote! {
                #tag_vis struct #q_name {
                    inner: #tag_name,
                }
            };
            quotes.push(q);
        }
        syn::Data::Enum(data_enum) => {
            for item in data_enum.variants.iter() {
                let attrs = &item.attrs;
                let ident = &item.ident;
                let fields = &item.fields;
                let q_name = format_ident!("{}{}", name_prefix, ident);
                let q = quote! {
                    #tag_vis struct #q_name #fields
                };
                quotes.push(q);
            }
        }
    };
    quote! {
        #(#quotes)*
    }
    .into()
}
