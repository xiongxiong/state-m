use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, DeriveInput, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

#[derive(Debug)]
struct KvAssocArgs(pub Type);

impl Parse for KvAssocArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        syn::parenthesized!(content in input);
        Ok(KvAssocArgs(content.parse()?))
    }
}

fn parse_kv_assoc_args(attrs: Vec<Attribute>) -> KvAssocArgs {
    let kv_assoc_attrs: Vec<&Attribute> = attrs
        .iter()
        .filter(|v| v.path().is_ident("kv_assoc"))
        .collect();
    match kv_assoc_attrs.len() {
        0 => panic!("[kv_assoc] attribute not found."),
        1 => {
            let attr = kv_assoc_attrs.get(0).unwrap();
            let args: KvAssocArgs = attr
                .parse_args()
                .unwrap_or_else(|e| panic!("[kv_assoc] attr invalid : {}", e));
            args
        }
        _ => panic!("[kv_assoc] attribute should not appear more than once."),
    }
}

#[proc_macro_attribute]
pub fn state_tag(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let k_name = &input.ident;
    let k_vis = &input.vis;
    let mut quotes: Vec<TokenStream2> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            for item in data_enum.variants {
                let ident = item.ident;
                let fields = item.fields;
                let q_name = format_ident!("{}{}", k_name, ident);
                let q = quote! {
                    #k_vis struct #q_name #fields
                };
                quotes.push(q);

                let kv_value_typ = parse_kv_assoc_args(item.attrs).0;
                let q_kv_assoc = quote! {
                    impl KvAssoc for #q_name {
                        type Value = #kv_value_typ;
                    }
                };
                quotes.push(q_kv_assoc);
            }
        }
        syn::Data::Struct(_) | syn::Data::Union(_) => {
            let kv_value_typ = parse_kv_assoc_args(input.attrs).0;
            let q = quote! {
                impl KvAssoc for #k_name {
                    type Value = #kv_value_typ;
                }
            };
            quotes.push(q);
        }
    };
    quote! {
        #(#quotes)*
    }
    .into()
}

#[proc_macro]
pub fn on_change(input: TokenStream) -> TokenStream {
    todo!()
}
