use macro_tools::{
    Assign, AttributeComponent, AttributePropertyComponent, AttributePropertyOptionalSyn, ct, qt,
    return_syn_err, syn_err,
};
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
pub fn kv_assoc(_attr: TokenStream, item: TokenStream) -> TokenStream {
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
pub fn on_change(_input: TokenStream) -> TokenStream {
    todo!()
}

#[derive(Debug, Default)]
struct StateTagAttr {
    pub assoc: AttributePropertyAssoc,
}

impl AttributeComponent for StateTagAttr {
    const KEYWORD: &'static str = "state_tag";

    fn from_meta(attr: &syn::Attribute) -> syn::Result<Self> {
        match attr.meta {
            syn::Meta::Path(ref _path) => Ok(Default::default()),
            syn::Meta::List(ref meta_list) => syn::parse2::<StateTagAttr>(meta_list.tokens.clone()),
            syn::Meta::NameValue(_) => return_syn_err!(
                attr,
                "Expects an attribute of format `#[ state_tag( assoc = Custom ) ) ]`. \nGot: {}",
                qt! { #attr }
            ),
        }
    }
}

impl Parse for StateTagAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut result = Self::default();
        let error = |ident: &syn::Ident| -> syn::Error {
            let known = ct::str::format!(
                "Known entries of attribute {} are: {}.",
                StateTagAttr::KEYWORD,
                AttributePropertyAssocMarker::KEYWORD
            );
            syn_err!(
                ident,
                r#"Expects an attribute of format '#[ state_tag( assoc = Custom ) ]'
                {known}
                But got:
                '{}'"#,
                qt! { #ident }
            )
        };
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                match ident.to_string().as_str() {
                    AttributePropertyAssoc::KEYWORD => {
                        result.assign(AttributePropertyAssoc::parse(input)?)
                    }
                    _ => return Err(error(&ident)),
                }
            } else {
                return Err(lookahead.error());
            }
            // optional trailing comma
            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(result)
    }
}

impl<IntoT> Assign<AttributePropertyAssoc, IntoT> for StateTagAttr
where
    IntoT: Into<AttributePropertyAssoc>,
{
    #[inline(always)]
    fn assign(&mut self, component: IntoT) {
        self.assoc = component.into()
    }
}

type AttributePropertyAssoc = AttributePropertyOptionalSyn<Type, AttributePropertyAssocMarker>;

#[derive(Clone, Copy, Debug, Default)]
struct AttributePropertyAssocMarker;

impl AttributePropertyComponent for AttributePropertyAssocMarker {
    const KEYWORD: &'static str = "assoc";
}

#[proc_macro_attribute]
pub fn state_tag(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let k_attr = parse_state_tag_attr(&input.attrs);
    let k_name = &input.ident;
    let k_vis = &input.vis;
    let mut quotes: Vec<TokenStream2> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            if k_attr.assoc.is_some() {
                panic!("Expects an attribute of format `#[state_tag]`.");
            }
            for item in data_enum.variants {
                let ident = &item.ident;
                let fields = &item.fields;
                let q_name = format_ident!("{}{}", k_name, ident);
                let q = quote! {
                    #k_vis struct #q_name #fields
                };
                quotes.push(q);

                let field_attr = parse_state_tag_attr(&item.attrs);
                match field_attr.assoc.internal() {
                    Some(kv_value_typ) => {
                        let q_kv_assoc = quote! {
                            impl state_m::KvAssoc for #q_name {
                                type Value = #kv_value_typ;
                            }
                        };
                        quotes.push(q_kv_assoc);
                    }
                    None => panic!(
                        "Expects an attribute of format `#[ state_tag( assoc = Custom ) ) ]`."
                    ),
                }
            }
        }
        syn::Data::Struct(_) | syn::Data::Union(_) => match k_attr.assoc.internal() {
            Some(kv_value_typ) => {
                let q = quote! {
                    impl state_m::KvAssoc for #k_name {
                        type Value = #kv_value_typ;
                    }
                };
                quotes.push(q);
            }
            None => {
                panic!("Expects an attribute of format `#[ state_tag( assoc = Custom ) ) ]`.")
            }
        },
    };
    quote! {
        #(#quotes)*
    }
    .into()
}

fn parse_state_tag_attr(attrs: &Vec<Attribute>) -> StateTagAttr {
    let mut state_tag_attr = StateTagAttr::default();
    for attr in attrs {
        if attr.path().is_ident(StateTagAttr::KEYWORD) {
            state_tag_attr = StateTagAttr::from_meta(attr).unwrap_or_else(|e| panic!("{}", e));
        }
    }
    state_tag_attr
}
