use macro_tools::{
    Assign, AttributeComponent, AttributePropertyComponent, AttributePropertyOptionalSyn,
    Itertools, ct, qt, return_syn_err, syn_err,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, DeriveInput, ExprClosure, Index, ReturnType, Token, Type, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token,
};

struct OnChangeInput {
    _paren_token: token::Paren,
    pub tag_typs: Punctuated<Type, Token![,]>,
    pub closure: ExprClosure,
}

impl Parse for OnChangeInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        Ok(OnChangeInput {
            _paren_token: parenthesized!(content in input),
            tag_typs: content.parse_terminated(Type::parse, Token![,])?,
            closure: input.parse()?,
        })
    }
}

#[proc_macro]
pub fn on_change(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as OnChangeInput);

    quote! {}.into()
}

#[derive(Debug, Default)]
struct KvAssocArgs {
    pub assoc: AttributePropertyAssoc,
}

impl AttributeComponent for KvAssocArgs {
    const KEYWORD: &'static str = "kv_assoc";

    fn from_meta(attr: &syn::Attribute) -> syn::Result<Self> {
        match attr.meta {
            syn::Meta::Path(ref _path) => Ok(Default::default()),
            syn::Meta::List(ref meta_list) => syn::parse2::<KvAssocArgs>(meta_list.tokens.clone()),
            syn::Meta::NameValue(_) => return_syn_err!(
                attr,
                "Expects an attribute of format `#[kv_assoc(assoc = Custom)]`. \nGot: {}",
                qt! { #attr }
            ),
        }
    }
}

impl Parse for KvAssocArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut result = Self::default();
        let error = |ident: &syn::Ident| -> syn::Error {
            let known = ct::str::format!(
                "Known entries of attribute {} are: {}.",
                KvAssocArgs::KEYWORD,
                AttributePropertyAssocMarker::KEYWORD
            );
            syn_err!(
                ident,
                r#"Expects an attribute of format '#[kv_assoc(assoc = Custom)]'
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

impl<IntoT> Assign<AttributePropertyAssoc, IntoT> for KvAssocArgs
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
    if !input.generics.params.is_empty() {
        panic!("Generics not supported.");
    }
    let i_attrs = &input.attrs;
    let i_ident = &input.ident;
    let i_vis = &input.vis;
    let mut quotes: Vec<TokenStream2> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            for item in &data_enum.variants {
                let q_attrs = q_attrs_except(&item.attrs, KvAssocArgs::KEYWORD);
                let v_ident = &item.ident;
                let v_fields = &item.fields;
                let t_name = format_ident!("{}{}", i_ident, v_ident);
                let q = match v_fields {
                    syn::Fields::Named(fields_named) => quote! {
                        #q_attrs #i_vis struct #t_name #fields_named
                    },
                    syn::Fields::Unnamed(fields_unnamed) => quote! {
                        #q_attrs #i_vis struct #t_name #fields_unnamed;
                    },
                    syn::Fields::Unit => quote! {
                        #q_attrs #i_vis struct #t_name;
                    },
                };
                quotes.push(q);

                let q_fr = match v_fields {
                    syn::Fields::Named(fields_named) => {
                        let mut q_params: Vec<TokenStream2> = fields_named
                            .named
                            .iter()
                            .map(|field| {
                                let ident = match field.ident {
                                    Some(ref ident) => ident.clone(),
                                    None => panic!("field should be named"),
                                };
                                vec![quote! {#ident: value.#ident}, quote! {,}]
                            })
                            .flatten()
                            .collect();
                        q_params.pop();
                        quote! {
                            #i_ident::#v_ident{#(#q_params)*}
                        }
                    }
                    syn::Fields::Unnamed(fields_unnamed) => {
                        let len = fields_unnamed.unnamed.len();
                        let mut q_params: Vec<TokenStream2> = (0..len)
                            .map(|i| {
                                let idx = Index::from(i);
                                vec![quote! {value.#idx}, quote! {,}]
                            })
                            .flatten()
                            .collect();
                        q_params.pop();
                        quote! {
                            #i_ident::#v_ident(#(#q_params)*)
                        }
                    }
                    syn::Fields::Unit => quote! {
                        #i_ident::#v_ident
                    },
                };
                let q_f = quote! {
                    impl From<#t_name> for #i_ident {
                        fn from(value: #t_name) -> #i_ident {
                            #q_fr
                        }
                    }
                };
                quotes.push(q_f);

                let args = kv_assoc_args(&item.attrs);
                match args.assoc.internal() {
                    Some(typ) => {
                        quotes.push(quote! {
                            impl state_m::KvAssoc for #t_name {
                                type Value = #typ;
                            }
                        });
                    }
                    None => panic!("Expects an attribute of format `#[kv_assoc(assoc = Custom)]`."),
                }
            }
            let q_attrs = q_attrs_except(i_attrs, KvAssocArgs::KEYWORD);
            let mut variants = data_enum.variants.clone();
            for item in variants.iter_mut() {
                item.attrs = attrs_except(&item.attrs, KvAssocArgs::KEYWORD);
            }
            quotes.push(quote! {
                #q_attrs #i_vis enum #i_ident {
                    #variants
                }
            });
        }
        syn::Data::Struct(data_struct) => {
            let q_attrs = q_attrs_except(i_attrs, KvAssocArgs::KEYWORD);
            let fields = data_struct.fields;
            let semi_colon = match data_struct.semi_token {
                Some(_) => quote! {;},
                None => quote! {},
            };
            let args = kv_assoc_args(&input.attrs);
            match args.assoc.internal() {
                Some(typ) => {
                    quotes.push(quote! {
                        #q_attrs #i_vis struct #i_ident #fields #semi_colon
                        impl state_m::KvAssoc for #i_ident {
                            type Value = #typ;
                        }
                    });
                }
                None => {
                    panic!("Expects an attribute of format `#[kv_assoc(assoc = Custom)]`.")
                }
            }
        }
        _ => panic!("Not supported."),
    };
    quote! {
        #(#quotes)*
    }
    .into()
}

fn kv_assoc_args(attrs: &Vec<Attribute>) -> KvAssocArgs {
    let mut args = KvAssocArgs::default();
    for attr in attrs {
        if attr.path().is_ident(KvAssocArgs::KEYWORD) {
            args = KvAssocArgs::from_meta(attr).unwrap_or_else(|e| {
                panic!(
                    "Unable to parse attribute [{}] : {}",
                    KvAssocArgs::KEYWORD,
                    e
                )
            });
        }
    }
    args
}

fn attrs_except(attrs: &Vec<Attribute>, except: &str) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|v| !v.path().is_ident(except))
        .cloned()
        .collect()
}

fn q_attrs_except(attrs: &Vec<Attribute>, except: &str) -> TokenStream2 {
    let attrs_n: Vec<_> = attrs_except(attrs, except);
    let mut qs: Vec<TokenStream2> = Vec::new();
    for attr in attrs_n {
        qs.push(quote! { #attr });
    }
    quote! {
        #(#qs)*
    }
}
