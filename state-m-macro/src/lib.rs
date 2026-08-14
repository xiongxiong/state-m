use macro_tools::{
    Assign, AttributeComponent, AttributePropertyComponent, AttributePropertyOptionalSyn,
    AttributePropertySyn, qt, return_syn_err, syn_err,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, DeriveInput, Expr, Field, FieldsNamed, FieldsUnnamed, Ident, Index, LitInt, Type,
    Visibility,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
};

const STATE_TAG_ARGS: &str = "#[state_tag(assoc = AssocType, label = \"AssocLabel\")]";

#[derive(Clone, Debug)]
struct StateTagArgs {
    pub assoc: AttributePropertyAssoc,
    pub label: AttributePropertyLabel,
}

impl AttributeComponent for StateTagArgs {
    const KEYWORD: &'static str = "state_tag";

    fn from_meta(attr: &syn::Attribute) -> syn::Result<Self> {
        match attr.meta {
            syn::Meta::List(ref meta_list) => syn::parse2::<StateTagArgs>(meta_list.tokens.clone()),
            _ => return_syn_err!(
                attr,
                "Expects an attribute of format `{STATE_TAG_ARGS}`. \nGot: {}",
                qt! { #attr }
            ),
        }
    }
}

impl Parse for StateTagArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let error = |ident: &syn::Ident| -> syn::Error {
            let known = format!(
                "Known entries of attribute {} are: {}, {}[optional].",
                StateTagArgs::KEYWORD,
                AttributePropertyAssocMarker::KEYWORD,
                AttributePropertyLabelMarker::KEYWORD,
            );
            syn_err!(
                ident,
                r#"Expects an attribute of format '{STATE_TAG_ARGS}'
                {known}
                But got:
                '{}'"#,
                qt! { #ident }
            )
        };
        let mut opt_assoc: Option<AttributePropertyAssoc> = None;
        let mut opt_label: Option<AttributePropertyLabel> = None;
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                match ident.to_string().as_str() {
                    AttributePropertyAssoc::KEYWORD => {
                        opt_assoc = Some(AttributePropertyAssoc::parse(input)?);
                    }
                    AttributePropertyLabel::KEYWORD => {
                        opt_label = Some(AttributePropertyLabel::parse(input)?);
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
        match opt_assoc {
            Some(assoc) => Ok(Self {
                assoc,
                label: opt_label.unwrap_or_default(),
            }),
            None => Err(syn_err!(
                "Attribute property '{}' is necessary.",
                AttributePropertyAssocMarker::KEYWORD
            )),
        }
    }
}

impl<IntoT> Assign<AttributePropertyAssoc, IntoT> for StateTagArgs
where
    IntoT: Into<AttributePropertyAssoc>,
{
    #[inline(always)]
    fn assign(&mut self, component: IntoT) {
        self.assoc = component.into()
    }
}

impl<IntoT> Assign<AttributePropertyLabel, IntoT> for StateTagArgs
where
    IntoT: Into<AttributePropertyLabel>,
{
    #[inline(always)]
    fn assign(&mut self, component: IntoT) {
        self.label = component.into()
    }
}

type AttributePropertyAssoc = AttributePropertySyn<Type, AttributePropertyAssocMarker>;

#[derive(Clone, Copy, Debug, Default)]
struct AttributePropertyAssocMarker;

impl AttributePropertyComponent for AttributePropertyAssocMarker {
    const KEYWORD: &'static str = "assoc";
}

type AttributePropertyLabel = AttributePropertyOptionalSyn<Expr, AttributePropertyLabelMarker>;

#[derive(Clone, Copy, Debug, Default)]
struct AttributePropertyLabelMarker;

impl AttributePropertyComponent for AttributePropertyLabelMarker {
    const KEYWORD: &'static str = "label";
}

fn state_tag_args(attrs: &Vec<Attribute>) -> StateTagArgs {
    for attr in attrs {
        if attr.path().is_ident(StateTagArgs::KEYWORD) {
            return StateTagArgs::from_meta(attr).unwrap_or_else(|e| {
                panic!(
                    "Unable to parse attribute [{}] : {}",
                    StateTagArgs::KEYWORD,
                    e
                )
            });
        }
    }
    panic!("Attribute {} is absent.", StateTagArgs::KEYWORD);
}

fn attrs_except(attrs: &Vec<Attribute>, excepts: Vec<&str>) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|v| excepts.iter().all(|e| !v.path().is_ident(e)))
        .cloned()
        .collect()
}

fn q_attrs_except(attrs: &Vec<Attribute>, excepts: Vec<&str>) -> TokenStream2 {
    let attrs_n: Vec<_> = attrs_except(attrs, excepts);
    let mut qs: Vec<_> = Vec::new();
    for attr in attrs_n {
        qs.push(quote! { #attr });
    }
    quote! {
        #(#qs)*
    }
}

#[proc_macro_derive(StateTag, attributes(state_tag))]
pub fn state_tag(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let i_attrs = &input.attrs;
    let i_ident = &input.ident;
    let i_vis = &input.vis;
    let i_gene = &input.generics;
    let i_where = match &i_gene.where_clause {
        Some(clause) => quote! {#clause},
        None => quote! {},
    };
    let i_g_idents = i_gene
        .params
        .iter()
        .map(|v| match v {
            syn::GenericParam::Type(type_param) => Some(type_param.ident.clone()),
            _ => None,
        })
        .filter(|v| v.is_some())
        .map(|v| v.unwrap())
        .collect::<Vec<_>>();
    let q_i_g_args = if i_g_idents.len() > 0 {
        let qs = itertools::intersperse(i_g_idents.iter().map(|v| quote! {#v}), quote! {,})
            .collect::<Vec<_>>();
        quote! {
            <#(#qs)*>
        }
    } else {
        quote! {}
    };
    let get_t_g_idents = |fields: &Punctuated<Field, Comma>| {
        fields
            .iter()
            .map(|v| match &v.ty {
                Type::Path(type_path) => type_path.path.segments.last().map(|s| s.ident.clone()),
                _ => None,
            })
            .filter(|v| v.is_some())
            .map(|v| v.unwrap())
            .collect::<Vec<_>>()
    };
    let get_q_t_g_args = |fields: &Punctuated<Field, Comma>| {
        let q_idents = itertools::intersperse(
            get_t_g_idents(fields)
                .iter()
                .filter(|v| i_g_idents.contains(v))
                .map(|v| quote! {#v})
                .collect::<Vec<_>>(),
            quote! {,},
        )
        .collect::<Vec<_>>();
        if q_idents.len() > 0 {
            quote! {<#(#q_idents)*>}
        } else {
            quote! {}
        }
    };
    let get_q_t_where = |items: &Punctuated<Field, Comma>| {
        let t_g_idents = get_t_g_idents(items);
        match &i_gene.where_clause {
            Some(clause) => {
                let q_pres = itertools::intersperse(
                    clause
                        .predicates
                        .iter()
                        .filter(|v| match v {
                            syn::WherePredicate::Type(predicate_type) => {
                                match &predicate_type.bounded_ty {
                                    Type::Path(type_path) => type_path
                                        .path
                                        .segments
                                        .last()
                                        .map(|s| t_g_idents.contains(&s.ident))
                                        .unwrap_or_default(),
                                    _ => false,
                                }
                            }
                            _ => false,
                        })
                        .cloned()
                        .map(|v| quote! { #v }),
                    quote! {,},
                )
                .collect::<Vec<_>>();
                if q_pres.len() > 0 {
                    quote! {
                        where #(#q_pres)*
                    }
                } else {
                    quote! {}
                }
            }
            None => quote! {},
        }
    };
    let mut quotes: Vec<_> = Vec::new();
    match input.data {
        syn::Data::Enum(data_enum) => {
            for item in &data_enum.variants {
                let q_attrs = q_attrs_except(&item.attrs, vec![StateTagArgs::KEYWORD]);
                let v_ident = &item.ident;
                let v_fields = &item.fields;
                let t_name = format_ident!("{}{}", i_ident, v_ident);
                let t_name_str = format!("{}{}", i_ident, v_ident);
                let (q, q_t_g_args, q_t_where) = match v_fields {
                    syn::Fields::Named(fields) => {
                        let q_t_g_args = get_q_t_g_args(&fields.named);
                        let q_t_where = get_q_t_where(&fields.named);
                        let fields_named_new = FieldsNamed {
                            named: fields
                                .named
                                .iter()
                                .map(|f| {
                                    let field = f.clone();
                                    Field {
                                        vis: Visibility::Public(Default::default()),
                                        ..field
                                    }
                                })
                                .collect(),
                            ..*fields
                        };
                        let q = quote! {
                            #[derive(Clone)]
                            #q_attrs #i_vis struct #t_name #q_t_g_args #q_t_where #fields_named_new
                        };
                        (q, q_t_g_args, q_t_where)
                    }
                    syn::Fields::Unnamed(fields) => {
                        let q_t_g_args = get_q_t_g_args(&fields.unnamed);
                        let q_t_where = get_q_t_where(&fields.unnamed);
                        let fields_unnamed_new = FieldsUnnamed {
                            unnamed: fields
                                .unnamed
                                .iter()
                                .map(|f| {
                                    let field = f.clone();
                                    Field {
                                        vis: Visibility::Public(Default::default()),
                                        ..field
                                    }
                                })
                                .collect(),
                            ..*fields
                        };
                        let q = quote! {
                            #[derive(Clone)]
                            #q_attrs #i_vis struct #t_name #q_t_g_args #fields_unnamed_new #q_t_where;
                        };
                        (q, q_t_g_args, q_t_where)
                    }
                    syn::Fields::Unit => {
                        let q = quote! {
                            #[derive(Clone, Default)]
                            #q_attrs #i_vis struct #t_name;
                        };
                        (q, quote! {}, quote! {})
                    }
                };
                quotes.push(q);

                // From
                let q_from_pat = match v_fields {
                    syn::Fields::Named(fields) => {
                        let q_params: Vec<_> = itertools::intersperse(
                            fields.named.iter().map(|field| {
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
                    syn::Fields::Unnamed(fields) => {
                        let len = fields.unnamed.len();
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
                let q_from = quote! {
                    impl #q_i_g_args std::convert::From<#t_name #q_t_g_args> for #i_ident #q_i_g_args #i_where {
                        fn from(value: #t_name #q_t_g_args) -> #i_ident #q_i_g_args {
                            #q_from_pat
                        }
                    }
                };
                quotes.push(q_from);

                // TryFrom
                let q_try_from_pat = match v_fields {
                    syn::Fields::Named(fields) => {
                        let q_params: Vec<_> = itertools::intersperse(
                            fields.named.iter().map(|field| {
                                let ident = match field.ident {
                                    Some(ref ident) => ident.clone(),
                                    None => panic!("field should be named"),
                                };
                                quote! {#ident}
                            }),
                            quote! {,},
                        )
                        .collect();
                        quote! {
                            #i_ident::#v_ident{#(#q_params)*}
                        }
                    }
                    syn::Fields::Unnamed(fields) => {
                        let len = fields.unnamed.len();
                        let q_params: Vec<_> = itertools::intersperse(
                            (0..len).map(|i| {
                                let idx = format_ident!("v_{i}");
                                quote! {#idx}
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
                let q_try_from_res = match v_fields {
                    syn::Fields::Named(fields) => {
                        let q_params: Vec<_> = itertools::intersperse(
                            fields.named.iter().map(|field| {
                                let ident = match field.ident {
                                    Some(ref ident) => ident.clone(),
                                    None => panic!("field should be named"),
                                };
                                quote! {#ident}
                            }),
                            quote! {,},
                        )
                        .collect();
                        quote! {
                            #t_name{#(#q_params)*}
                        }
                    }
                    syn::Fields::Unnamed(fields) => {
                        let len = fields.unnamed.len();
                        let q_params: Vec<_> = itertools::intersperse(
                            (0..len).map(|i| {
                                let idx = format_ident!("v_{i}");
                                quote! {#idx}
                            }),
                            quote! {,},
                        )
                        .collect();
                        quote! {
                            #t_name(#(#q_params)*)
                        }
                    }
                    syn::Fields::Unit => quote! {
                        #t_name
                    },
                };
                let q_try_from = quote! {
                    impl #q_i_g_args std::convert::TryFrom<#i_ident #q_i_g_args> for #t_name #q_t_g_args #i_where {
                        type Error = &'static str;

                        fn try_from(value: #i_ident #q_i_g_args) -> Result<Self, Self::Error> {
                            match value {
                                #q_try_from_pat => Ok(#q_try_from_res),
                                _ => Err("Unsupported convertion.")
                            }
                        }
                    }
                };
                quotes.push(q_try_from);

                // KvAssoc
                let args = state_tag_args(&item.attrs);
                let typ = args.clone().assoc.internal();
                quotes.push(quote! {
                    impl #q_t_g_args KvAssoc for #t_name #q_t_g_args #q_t_where {
                        type Value = #typ;
                    }
                });

                // Debug
                let q_debug = match args.label.internal() {
                    Some(expr) => {
                        quote! {
                            impl #q_t_g_args std::fmt::Debug for #t_name #q_t_g_args #q_t_where {
                                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                                    write!(f, "{}", #expr)
                                }
                            }
                        }
                    }
                    None => {
                        quote! {
                            impl #q_t_g_args std::fmt::Debug for #t_name #q_t_g_args #q_t_where {
                                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                                    write!(f, "{}", #t_name_str)
                                }
                            }
                        }
                    }
                };
                quotes.push(q_debug);

                // IsTag
                let q_is_tag_pat = match v_fields {
                    syn::Fields::Named(_) => {
                        quote! {
                            #i_ident::#v_ident(_)
                        }
                    }
                    syn::Fields::Unnamed(fields) => {
                        let len = fields.unnamed.len();
                        let qs: Vec<_> = itertools::intersperse(
                            (0..len).map(|_| {
                                quote! {_}
                            }),
                            quote! {,},
                        )
                        .collect();
                        quote! {
                            #i_ident::#v_ident(#(#qs)*)
                        }
                    }
                    syn::Fields::Unit => quote! {
                        #i_ident::#v_ident
                    },
                };
                let q_is_tag = quote! {
                    impl #q_i_g_args KeyIsTag<#t_name #q_t_g_args> for #i_ident #q_i_g_args #i_where {
                        fn predicate(&self) -> bool {
                            match self {
                                #q_is_tag_pat => true,
                                _ => false
                            }
                        }
                    }
                };
                quotes.push(q_is_tag);
            }
        }
        syn::Data::Struct(data_struct) => {
            let q_attrs = q_attrs_except(i_attrs, vec![StateTagArgs::KEYWORD]);
            let fields = data_struct.fields;
            let semi_colon = match data_struct.semi_token {
                Some(_) => quote! {;},
                None => quote! {},
            };
            let args = state_tag_args(&input.attrs);
            let typ = args.clone().assoc.internal();
            quotes.push(quote! {
                #q_attrs #i_vis struct #i_ident #q_i_g_args #fields #semi_colon
                impl #q_i_g_args KvAssoc for #i_ident #q_i_g_args #i_where {
                    type Value = #typ;
                }
            });
        }
        _ => panic!("Not supported."),
    };
    quote! {
        #(#quotes)*
    }
    .into()
}

#[proc_macro]
pub fn sm_watch(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 0, "Input number should larger than zero.");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                StateChange<#typ>,
            }
        })
        .collect();
    let vec_tags: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            quote! {
                #name.clone().into()
            }
        }),
        quote! {,},
    )
    .collect();
    let decl_vars: Vec<_> = (0..n)
        .map(|i| {
            let tag_name = format_ident!("tag_{}", i);
            let handle_name = format_ident!("handle_{}", i);
            let rx_name = format_ident!("rx_{}", i);
            let token_name = format_ident!("token_{}", i);
            quote! {
                    let #handle_name = self.get_handle(#tag_name.clone())?;
                    let (mut #rx_name, #token_name) = #handle_name.fanout();
            }
        })
        .collect();
    let all_state_names: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("state_{}", i);
            quote! {
                #name
            }
        }),
        quote! {,},
    )
    .collect();
    let calc_all_states = |idx: usize| {
        itertools::intersperse(
            (0..n).map(|i| {
                if i != idx {
                    let handle_name = format_ident!("handle_{}", i);
                    quote! {
                        StateChange::UnChange(#handle_name.state())
                    }
                } else {
                    quote! {
                        StateChange::Change(s_cur, s_old)
                    }
                }
            }),
            quote! {,},
        )
        .collect::<Vec<_>>()
    };
    let sel_tokens: Vec<_> = (0..n)
        .map(|i| {
            let token_name = format_ident!("token_{}", i);
            quote! {
                _ = #token_name.cancelled() => break,
            }
        })
        .collect();
    let sel_recvs: Vec<_> = (0..n)
        .map(|i| {
            let all_states = calc_all_states(i);
            let tag_name = format_ident!("tag_{}", i);
            let rx_name = format_ident!("rx_{}", i);
            quote! {
                r = #rx_name.recv() => {
                    match r {
                        Ok((s_cur, s_old)) => {
                            let mut states = (#(#all_states)*);
                            let (#(#all_state_names)*) = states;
                            if let Err(e) = func(#(#all_state_names)*, #tag_name.clone().into()).await {
                                tracing::error!("{id} | {:?} | watch error -- {e:?}", #tag_name);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        })
        .collect();
    let get_q_method = move |m_name: Ident| {
        quote! {
            pub async fn #m_name<#(#tag_typs)*, F>(&self, #(#tag_params)*, func: F) -> Result<(), TagUsageError<K>>
            where
                #(#tag_typ_cons)*
                F: 'static
                    + Fn(
                        #(#fn_params_typ)* K
                    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
                    + Send,
            {
                let tags: Vec<K> = vec![#(#vec_tags)*];
                if !tags.iter().duplicates().collect::<Vec<_>>().is_empty() {
                    return Err(TagUsageError::DuplicatedTags);
                }
                let id = self.0.clone();
                #(#decl_vars)*
                tokio::spawn(async move {
                    tracing::info!("{id} | watch_{} | {tags:?} -- start", #n);
                    loop {
                        tokio::select! {
                            biased;
                            #(#sel_tokens)*
                            #(#sel_recvs)*
                        }
                    }
                    tracing::info!("{id} | watch_{} | {tags:?} -- close", #n);
                });
                Ok(())
            }
        }
    };
    let norm_method = get_q_method(format_ident!("watch_{n}"));
    quote! {
        #norm_method
    }
    .into()
}

#[proc_macro]
pub fn watch_decl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 0, "Input number should larger than zero.");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                StateChange<#typ>,
            }
        })
        .collect();
    let get_q_method = |m_name: Ident| {
        quote! {
            async fn #m_name<#(#tag_typs)*, F>(&self, #(#tag_params)*, func: F) -> Result<(), TagUsageError<Self::K>>
            where
                #(#tag_typ_cons)*
                F: 'static
                    + Fn(
                        #(#fn_params_typ)* Self::K
                    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
                    + Send;
        }
    };
    let norm_method = get_q_method(format_ident!("watch_{n}"));
    quote! {
        /// Watch multiple state readers simultaneously, state events from these readers arrived in queue.
        #norm_method
    }
    .into()
}

#[proc_macro]
pub fn watch_impl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 0, "Input number should larger than zero.");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                StateChange<#typ>,
            }
        })
        .collect();
    let params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            quote! {
                #name
            }
        }),
        quote! {,},
    )
    .collect();
    let get_q_method = move |m_name: Ident| {
        quote! {
            async fn #m_name<#(#tag_typs)*, F>(&self, #(#tag_params)*, func: F) -> Result<(), TagUsageError<Self::K>>
            where
                #(#tag_typ_cons)*
                F: 'static
                    + Fn(
                        #(#fn_params_typ)* Self::K
                    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
                    + Send
            {
                self.state_machine().#m_name(#(#params)*, func).await
            }
        }
    };
    let norm_method = get_q_method(format_ident!("watch_{n}"));
    quote! {
        #norm_method
    }
    .into()
}

#[proc_macro]
pub fn sm_merge_reader(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than one.");
    let method_name = format_ident!("merge_reader_{n}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ::Value,
            }
        })
        .collect();
    let vec_tags: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            quote! {
                #name.clone().into()
            }
        }),
        quote! {,},
    )
    .collect();
    let decl_vars: Vec<_> = (0..n)
        .map(|i| {
            let tag_name = format_ident!("tag_{}", i);
            let handle_name = format_ident!("handle_{}", i);
            let rx_name = format_ident!("rx_{}", i);
            let token_name = format_ident!("token_{}", i);
            quote! {
                    let #handle_name = self.get_handle(#tag_name.clone())?;
                    let (mut #rx_name, #token_name) = #handle_name.fanout();
            }
        })
        .collect();
    let chan_decl = {
        quote! {
            let (tx, _) = tokio::sync::broadcast::channel(capacity);
            let tx_c = tx.clone();
        }
    };
    let sel_tokens: Vec<_> = (0..n)
        .map(|i| {
            let token_name = format_ident!("token_{}", i);
            quote! {
                _ = #token_name.cancelled() => break,
            }
        })
        .collect();
    let calc_state_decls = |idx| {
        (0..n)
            .map(|i| {
                let handle_name = format_ident!("handle_{}", i);
                let state_name = format_ident!("state_{}", i);
                if i != idx {
                    quote! {
                        let #state_name = #handle_name.state();
                    }
                } else {
                    quote! {
                        let #state_name = s_cur;
                    }
                }
            })
            .collect::<Vec<_>>()
    };
    let all_state_names: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let state_name = format_ident!("state_{}", i);
            quote! {
                #state_name.value
            }
        }),
        quote! {,},
    )
    .collect();
    let sel_recvs: Vec<_> = (0..n)
        .map(|i| {
            let state_decls = calc_state_decls(i);
            let rx_name = format_ident!("rx_{}", i);
            quote! {
                r = #rx_name.recv() => {
                    match r {
                        Ok((s_cur, _)) => {
                            #(#state_decls)*
                            let value = func(#(#all_state_names)*);
                            let event = StateEvent {
                                state: State {
                                    value,
                                    timestamp: chrono::Utc::now(),
                                },
                                is_touch: false,
                                close_handle: None,
                            };
                            if tx_c.send(event).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        })
        .collect();
    quote! {
        pub async fn #method_name<#(#tag_typs)*, S, F>(&self, #(#tag_params)*, capacity: usize, func: F) -> Result<Reader<S>, TagUsageError<K>>
        where
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#fn_params_typ)*) -> S + Send,
        {
            let tags: Vec<K> = vec![#(#vec_tags)*];
            if !tags.iter().duplicates().collect::<Vec<_>>().is_empty() {
                return Err(TagUsageError::DuplicatedTags);
            }
            let id = self.0.clone();
            #(#decl_vars)*
            #chan_decl
            tokio::spawn(async move {
                tracing::info!("{id} | merge_reader_{} | {tags:?} -- start", #n);
                loop {
                    tokio::select! {
                        biased;
                        #(#sel_tokens)*
                        #(#sel_recvs)*
                    }
                }
                tracing::info!("{id} | merge_reader_{} | {tags:?} -- close", #n);
            });
            Ok(Reader::new(capacity, tx))
        }
    }.into()
}

#[proc_macro]
pub fn merge_reader_decl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than zero.");
    let method_name = format_ident!("merge_reader_{n}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ::Value,
            }
        })
        .collect();
    quote! {
        /// Merge multiple state readers into one.
        async fn #method_name<#(#tag_typs)*, S, F>(&self, #(#tag_params)*, capacity: usize, func: F) -> Result<Reader<S>, TagUsageError<Self::K>>
        where
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#fn_params_typ)*) -> S + Send;
    }.into()
}

#[proc_macro]
pub fn merge_reader_impl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than zero.");
    let method_name = format_ident!("merge_reader_{n}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let tag_params: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            let typ = format_ident!("T{}", i);
            quote! {
                #name: #typ
            }
        }),
        quote! {,},
    )
    .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
                #typ::Value: AsState,
            }
        })
        .collect();
    let fn_params_typ: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ::Value,
            }
        })
        .collect();
    let tag_names: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let name = format_ident!("tag_{}", i);
            quote! {
                #name
            }
        }),
        quote! {,},
    )
    .collect();
    quote! {
        async fn #method_name<#(#tag_typs)*, S, F>(&self, #(#tag_params)*, capacity: usize, func: F) -> Result<Reader<S>, TagUsageError<Self::K>>
        where
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#fn_params_typ)*) -> S + Send {
                self.state_machine().#method_name(#(#tag_names)*, capacity, func).await
            }
    }.into()
}

#[proc_macro]
pub fn sm_split_reader(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than one.");
    let method_name = format_ident!("split_reader_{n}");
    let state_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let reader_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {Reader<#typ>}
        }),
        quote! {,},
    )
    .collect();
    let state_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {
                #typ: AsState,
            }
        })
        .collect();
    let decl_vars: Vec<_> = (0..n)
        .map(|i| {
            let tx_name = format_ident!("tx_{}", i);
            let tx_name_c = format_ident!("tx_{}_c", i);
            quote! {
                let (#tx_name, _) = tokio::sync::broadcast::channel(capacity);
                let #tx_name_c = #tx_name.clone();
            }
        })
        .collect();
    let value_names: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let value_name = format_ident!("v_{}", i);
            quote! { #value_name }
        }),
        quote! {,},
    )
    .collect();
    let send_states: Vec<_> = (0..n)
        .map(|i| {
            let value_name = format_ident!("v_{}", i);
            let event_name = format_ident!("e_{}", i);
            let tx_name_c = format_ident!("tx_{}_c", i);
            quote! {
                let #event_name = StateEvent {
                    state: State {
                        value: #value_name,
                        timestamp: s_cur.timestamp.clone(),
                    },
                    is_touch: false,
                    close_handle: None,
                };
                if #tx_name_c.send(#event_name).is_err() {
                    break;
                }
            }
        })
        .collect();
    let res_readers: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let tx_name = format_ident!("tx_{}", i);
            quote! {
                Reader::new(capacity, #tx_name)
            }
        }),
        quote! {,},
    )
    .collect();
    quote!{
        pub async fn #method_name<T, F, #(#state_typs)*>(&self, tag: T, func: F) -> Result<(#(#reader_typs)*), GetHandleError<K>>
        where
            T: 'static + Clone + Debug + Into<K> + KvAssoc + Send + Sync,
            T::Value: AsState,
            F: 'static + Fn(T::Value) -> (#(#state_typs)*) + Send,
            #(#state_typ_cons)*
        {
            let id = self.0.clone();
            let handle = self.get_handle(tag.clone())?;
            let capacity = handle.capacity();
            let (mut rx, token) = handle.fanout();
            #(#decl_vars)*
            let res_typ_name = std::any::type_name::<(#(#reader_typs)*)>();
            tokio::spawn(async move {
                tracing::info!("{id} | split_reader_{} | {tag:?} | {res_typ_name} -- start", #n);
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        r = rx.recv() => {
                            match r {
                                Ok((s_cur, _)) => {
                                    let (#(#value_names)*) = func(s_cur.value);
                                    #(#send_states)*
                                },
                                Err(_) => break,
                            }
                        }
                    }
                }
                tracing::info!("{id} | split_reader_{} | {tag:?} | {res_typ_name} -- close", #n);
            });
            Ok((#(#res_readers)*))
        }
    }.into()
}

#[proc_macro]
pub fn split_reader_decl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than one.");
    let method_name = format_ident!("split_reader_{n}");
    let state_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let reader_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {Reader<#typ>}
        }),
        quote! {,},
    )
    .collect();
    let state_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {
                #typ: AsState,
            }
        })
        .collect();
    quote!{
        /// Split a state reader into multiple state readers.
        async fn #method_name<T, F, #(#state_typs)*>(&self, tag: T, func: F) -> Result<(#(#reader_typs)*), GetHandleError<Self::K>>
        where
            T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
            T::Value: AsState,
            F: 'static + Fn(T::Value) -> (#(#state_typs)*) + Send,
            #(#state_typ_cons)*;
    }.into()
}

#[proc_macro]
pub fn split_reader_impl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    assert!(n > 1, "Input number should larger than one.");
    let method_name = format_ident!("split_reader_{n}");
    let state_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let reader_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {Reader<#typ>}
        }),
        quote! {,},
    )
    .collect();
    let state_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("S{}", i);
            quote! {
                #typ: AsState,
            }
        })
        .collect();
    quote!{
        async fn #method_name<T, F, #(#state_typs)*>(&self, tag: T, func: F) -> Result<(#(#reader_typs)*), GetHandleError<Self::K>>
        where
            T: 'static + Clone + Debug + Into<Self::K> + KvAssoc + Send + Sync,
            T::Value: AsState,
            F: 'static + Fn(T::Value) -> (#(#state_typs)*) + Send,
            #(#state_typ_cons)* {
                self.state_machine().#method_name(tag, func).await
            }
    }.into()
}

#[proc_macro]
pub fn sm_merge_same(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    let method_name_str = if n > 1 {
        format!("merge_same_{n}")
    } else {
        format!("merge_same")
    };
    let method_name = format_ident!("{method_name_str}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let key_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                K: KeyIsTag<#typ>,
            }
        })
        .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Eq + std::hash::Hash + Into<K> + KvAssoc + PartialEq + Send + Sync + TryFrom<K, Error = &'static str>,
                #typ::Value: AsState,
            }
        })
        .collect();
    let func_arg_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                Vec<(#typ, #typ::Value)>
            }
        }),
        quote! {,},
    )
    .collect();
    let all_tag_typ_names: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{i}");
            quote! {
                std::any::type_name::<#typ>().into()
            }
        }),
        quote! {,},
    )
    .collect();
    let decl_vars = (0..n).map(|i| {
        let typ = format_ident!("T{i}");
        let tags = format_ident!("tags_{i}");
        let handles = format_ident!("handles_{i}");
        let stream_map = format_ident!("stream_map_{i}");
        quote! {
            let #tags = self
                .1
                .iter()
                .filter(|v| KeyIsTag::<#typ>::predicate(v.key()))
                .map(|v| #typ::try_from(v.key().clone()).unwrap())
                .collect::<Vec<_>>();
            if #tags.is_empty() {
                return Err(MergeSameError::NoTagOfTyp(
                    std::any::type_name::<T0>().into(),
                ));
            }
            let #handles: Vec<_> = #tags
                .iter()
                .map(|t| self.get_handle(t.clone()).unwrap())
                .collect();
            let mut #stream_map: StreamMap<usize, _> = StreamMap::new();
            for (j, h) in #handles.iter().enumerate() {
                #stream_map.insert(j, BroadcastStream::new(h.fanout().0));
            }
        }
    });
    let states_names = itertools::intersperse(
        (0..n).map(|i| {
            let states = format_ident!("states_{i}");
            quote! {#states}
        }),
        quote! {,},
    )
    .collect::<Vec<_>>();
    let decl_get_all_states = {
        let decl_states = (0..n).map(|i| {
            let states = format_ident!("states_{i}");
            let tags = format_ident!("tags_{i}");
            let handles = format_ident!("handles_{i}");
            quote! {
                let #states = #tags
                    .iter()
                    .zip(#handles.iter())
                    .map(|(t, h)| (t.clone(), h.value()))
                    .collect::<Vec<_>>();
            }
        });
        quote! {
            let get_all_states = || {
                #(#decl_states)*
                (#(#states_names)*)
            };
        }
    };
    let sel_branchs = (0..n).map(|i| {
        let stream_map = format_ident!("stream_map_{i}");
        quote! {
            r = #stream_map.next() => {
                match r {
                    Some((j, Ok(_))) => {
                        let (#(#states_names)*) = get_all_states();
                        let value = func(#(#states_names)*);
                        let event = StateEvent {
                            state: State {
                                value,
                                timestamp: chrono::Utc::now(),
                            },
                            is_touch: false,
                            close_handle: None,
                        };
                        if tx_c.send(event).is_err() {
                            break;
                        }
                    }
                    _ => break
                }
            }
        }
    });
    quote! {
        pub async fn #method_name<#(#tag_typs)*, S, F>(&self, capacity: usize, func: F) -> Result<Reader<S>, MergeSameError<K>>
        where
            #(#key_typ_cons)*
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#func_arg_typs)*) -> S + Send,
        {
            let all_tag_typ_names: Vec<String> = vec![#(#all_tag_typ_names)*];
            if !all_tag_typ_names.iter().duplicates().collect::<Vec<_>>().is_empty() {
                return Err(MergeSameError::DuplicatedTagTyps.into());
            }
            #(#decl_vars)*
            #decl_get_all_states
            let id = self.0.clone();
            let (tx, _) = tokio::sync::broadcast::channel(capacity);
            let tx_c = tx.clone();
            tracing::info!("{id} | {} -- start", #method_name_str);
            loop {
                tokio::select! {
                    #(#sel_branchs)*
                }
            }
            tracing::info!("{id} | {} -- close", #method_name_str);
            Ok(Reader::new(capacity, tx))
        }
    }
    .into()
}

#[proc_macro]
pub fn merge_same_decl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    let method_name_str = if n > 1 {
        format!("merge_same_{n}")
    } else {
        format!("merge_same")
    };
    let method_name = format_ident!("{method_name_str}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let key_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                Self::K: KeyIsTag<#typ>,
            }
        })
        .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Eq + std::hash::Hash + Into<Self::K> + KvAssoc + PartialEq + Send + Sync + TryFrom<Self::K, Error = &'static str>,
                #typ::Value: AsState,
            }
        })
        .collect();
    let func_arg_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                Vec<(#typ, #typ::Value)>
            }
        }),
        quote! {,},
    )
    .collect();
    quote! {
        async fn #method_name<#(#tag_typs)*, S, F>(&self, capacity: usize, func: F) -> Result<Reader<S>, MergeSameError<Self::K>>
        where
            #(#key_typ_cons)*
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#func_arg_typs)*) -> S + Send;
    }
    .into()
}

#[proc_macro]
pub fn merge_same_impl(input: TokenStream) -> TokenStream {
    let lit_n = parse_macro_input!(input as LitInt);
    let n = lit_n
        .base10_parse::<usize>()
        .expect("Input can only be a number of type [usize].");
    let method_name_str = if n > 1 {
        format!("merge_same_{n}")
    } else {
        format!("merge_same")
    };
    let method_name = format_ident!("{method_name_str}");
    let tag_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {#typ}
        }),
        quote! {,},
    )
    .collect();
    let key_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                Self::K: KeyIsTag<#typ>,
            }
        })
        .collect();
    let tag_typ_cons: Vec<_> = (0..n)
        .map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                #typ: 'static + Clone + Debug + Eq + std::hash::Hash + Into<Self::K> + KvAssoc + PartialEq + Send + Sync + TryFrom<Self::K, Error = &'static str>,
                #typ::Value: AsState,
            }
        })
        .collect();
    let func_arg_typs: Vec<_> = itertools::intersperse(
        (0..n).map(|i| {
            let typ = format_ident!("T{}", i);
            quote! {
                Vec<(#typ, #typ::Value)>
            }
        }),
        quote! {,},
    )
    .collect();
    quote! {
        async fn #method_name<#(#tag_typs)*, S, F>(&self, capacity: usize, func: F) -> Result<Reader<S>, MergeSameError<Self::K>>
        where
            #(#key_typ_cons)*
            #(#tag_typ_cons)*
            S: AsState,
            F: 'static + Fn(#(#func_arg_typs)*) -> S + Send
        {
            self.state_machine().#method_name(capacity, func).await
        }
    }
    .into()
}
