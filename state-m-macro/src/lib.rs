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

#[proc_macro_attribute]
pub fn state_tag(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let k_name = &input.ident;
    let k_vis = &input.vis;
    let mut quotes: Vec<TokenStream2> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            for item in data_enum.variants {
                let ident = &item.ident;
                let fields = &item.fields;
                let q_name = format_ident!("{}{}", k_name, ident);
                let q = quote! {
                    #k_vis struct #q_name #fields
                };
                quotes.push(q);

                for attr in &item.attrs {
                    if attr.path().is_ident("kv_assoc") {
                        let args: KvAssocArgs = attr
                            .parse_args()
                            .unwrap_or_else(|e| panic!("[kv_assoc] attr invalid : {}", e));
                    }
                }
            }
        }
        _ => panic!("[state_tag] attribute macro can only be used on enum data type."),
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
