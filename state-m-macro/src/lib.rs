use macro_tools::{
    Assign, AttributeComponent, AttributePropertyComponent, AttributePropertyOptionalSyn, ct, qt,
    return_syn_err, syn_err,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, DeriveInput, Index, LitInt, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

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
    let mut qs: Vec<_> = Vec::new();
    for attr in attrs_n {
        qs.push(quote! { #attr });
    }
    quote! {
        #(#qs)*
    }
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
    let mut quotes: Vec<_> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            for item in &data_enum.variants {
                let q_attrs = q_attrs_except(&item.attrs, KvAssocArgs::KEYWORD);
                let v_ident = &item.ident;
                let v_fields = &item.fields;
                let t_name = format_ident!("{}{}", i_ident, v_ident);
                let q = match v_fields {
                    syn::Fields::Named(fields_named) => quote! {
                        #[derive(Clone, Debug)]
                        #q_attrs #i_vis struct #t_name #fields_named
                    },
                    syn::Fields::Unnamed(fields_unnamed) => quote! {
                        #[derive(Clone, Debug)]
                        #q_attrs #i_vis struct #t_name #fields_unnamed;
                    },
                    syn::Fields::Unit => quote! {
                        #[derive(Clone, Debug)]
                        #q_attrs #i_vis struct #t_name;
                    },
                };
                quotes.push(q);

                let q_fr = match v_fields {
                    syn::Fields::Named(fields_named) => {
                        let q_params: Vec<_> = itertools::intersperse(
                            fields_named.named.iter().map(|field| {
                                let ident = match field.ident {
                                    Some(ref ident) => ident.clone(),
                                    None => panic!("field should be named"),
                                };
                                quote! {#ident: value.#ident}
                            }),
                            quote! {,},
                        )
                        .collect();
                        quote! {
                            #i_ident::#v_ident{#(#q_params)*}
                        }
                    }
                    syn::Fields::Unnamed(fields_unnamed) => {
                        let len = fields_unnamed.unnamed.len();
                        let q_params: Vec<_> = itertools::intersperse(
                            (0..len).map(|i| {
                                let idx = Index::from(i);
                                quote! {value.#idx}
                            }),
                            quote! {,},
                        )
                        .collect();
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

#[proc_macro]
pub fn sm_join(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number");
    assert!(n > 0, "Input number should larger than zero.");
    let method_name = format_ident!("join_{n}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i + 1);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i + 1);
            let typ = format_ident!("T{}", i + 1);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i + 1);
            quote! {
                #typ: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
                #typ::Value: 'static + AsState + Send,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i + 1);
            quote! {
                Option<(State<#typ::Value>, State<#typ::Value>)>,
            }
        })
        .collect();
    let vec_tags: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i + 1);
            quote! {
                #name.clone().into()
            }
        }),
        quote! {,},
    )
    .collect();
    let decl_vars: Vec<_> = (0..n)
        .map(|i| {
            let tag_name = format_ident!("tag_{}", i + 1);
            let handle_name = format_ident!("handle_{}", i + 1);
            let rx_name = format_ident!("rx_{}", i + 1);
            let token_name = format_ident!("token_{}", i + 1);
            quote! {
                    let #handle_name = self.get_handle(#tag_name)?;
                    let (mut #rx_name, #token_name) = #handle_name.fanout();
            }
        })
        .collect();
    let sel_tokens: Vec<_> = (0..n)
        .map(|i| {
            let token_name = format_ident!("token_{}", i + 1);
            quote! {
                _ = #token_name.cancelled() => break,
            }
        })
        .collect();
    let sel_recvs: Vec<_> = (0..n)
        .map(|i| {
            let rx_name = format_ident!("rx_{}", i + 1);
            let param = quote! {Some(p),};
            let prev_params: Vec<_> = (0..i).map(|_| quote! { None, }).collect();
            let post_params: Vec<_> = ((i + 1)..n).map(|_| quote! { None, }).collect();
            quote! {
                r = #rx_name.recv() => {
                    match r {
                        Ok(p) => {
                            if let Err(e) = func(#(#prev_params)* #param #(#post_params)*).await {
                                tracing::error!("join error -- {e:?}");
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        })
        .collect();
    quote! {
        pub async fn #method_name<#(#tag_typs)*, F>(&self, #(#tag_params)*, func: F) -> anyhow::Result<()>
        where
            #(#tag_typ_cons)*
            F: 'static
                + Fn(
                    #(#fn_params_typ)*
                ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
                + Send,
        {
            let tags: Vec<K> = vec![#(#vec_tags)*];
            assert!(
                tags.iter().duplicates().collect::<Vec<_>>().is_empty(),
                "Should not use duplicate tags."
            );
            #(#decl_vars)*
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        #(#sel_tokens)*
                        #(#sel_recvs)*
                    }
                }
            });
            Ok(())
        }
    }
    .into()
}
