//! `#[derive(Event)]` expansion.

use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, LitInt, LitStr};

/// Parsed `#[event(name = "...", version = N)]` attribute.
struct EventAttr {
    name:    LitStr,
    version: u16,
}

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input,
            "#[derive(Event)] can only be applied to an enum: each variant \
             maps to one dotted wire name (e.g. `Opened` -> \
             \"account.opened\")",
        ));
    };

    if let Some(param) = input.generics.params.iter().next() {
        return Err(syn::Error::new_spanned(
            param,
            "#[derive(Event)] does not support generic events: the wire \
             format is fixed per concrete type",
        ));
    }

    let attr = parse_event_attr(&input)?;
    let enum_ident = &input.ident;
    let prefix = attr.name.value();
    let version = attr.version;

    let mut name_arms = Vec::new();
    let mut encode_arms = Vec::new();
    let mut decode_arms = Vec::new();
    let mut fingerprint_arms = Vec::new();
    let mut wire_names = Vec::new();

    for variant in &data.variants {
        let vident = &variant.ident;
        let wire = format!("{prefix}.{}", to_snake_case(&vident.to_string()));
        let wire_lit = LitStr::new(&wire, vident.span());
        wire_names.push(wire_lit.clone());

        let fingerprint = variant_fingerprint(&wire, version, &variant.fields);

        match &variant.fields {
            Fields::Named(named) if !named.named.is_empty() => {
                let fnames: Vec<&Ident> = named
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().unwrap())
                    .collect();
                let ftypes: Vec<&syn::Type> =
                    named.named.iter().map(|f| &f.ty).collect();

                name_arms.push(quote! {
                    #enum_ident::#vident { .. } => #wire_lit
                });

                encode_arms.push(quote! {
                    #enum_ident::#vident { #(#fnames),* } => {
                        #[derive(::serde::Serialize)]
                        struct __Payload<'__a> {
                            #(#fnames: &'__a #ftypes),*
                        }
                        __mess_encode(&__Payload { #(#fnames),* })
                    }
                });

                decode_arms.push(quote! {
                    #wire_lit => {
                        #[derive(::serde::Deserialize)]
                        struct __Payload {
                            #(#fnames: #ftypes),*
                        }
                        let __Payload { #(#fnames),* } =
                            __mess_decode::<__Payload>(name, data)?;
                        ::core::result::Result::Ok(
                            #enum_ident::#vident { #(#fnames),* },
                        )
                    }
                });
            }
            Fields::Unnamed(unnamed) if !unnamed.unnamed.is_empty() => {
                let ftypes: Vec<&syn::Type> =
                    unnamed.unnamed.iter().map(|f| &f.ty).collect();
                let binds: Vec<Ident> = (0..ftypes.len())
                    .map(|i| format_ident!("__f{i}"))
                    .collect();

                name_arms.push(quote! {
                    #enum_ident::#vident(..) => #wire_lit
                });

                // Tuple variants have no field names, so their payload is a
                // positional array. (Named variants get name-keyed maps.)
                encode_arms.push(quote! {
                    #enum_ident::#vident(#(#binds),*) => {
                        __mess_encode(&( #(#binds ,)* ))
                    }
                });

                decode_arms.push(quote! {
                    #wire_lit => {
                        let ( #(#binds ,)* ): ( #(#ftypes ,)* ) =
                            __mess_decode(name, data)?;
                        ::core::result::Result::Ok(
                            #enum_ident::#vident(#(#binds),*),
                        )
                    }
                });
            }
            // Unit variant, or an empty `{}` / `()` variant: no payload.
            fields => {
                let ctor = match fields {
                    Fields::Named(_) => quote! { #enum_ident::#vident {} },
                    Fields::Unnamed(_) => quote! { #enum_ident::#vident() },
                    Fields::Unit => quote! { #enum_ident::#vident },
                };
                let pat = match fields {
                    Fields::Named(_) => quote! { #enum_ident::#vident { .. } },
                    Fields::Unnamed(_) => quote! { #enum_ident::#vident(..) },
                    Fields::Unit => quote! { #enum_ident::#vident },
                };
                name_arms.push(quote! { #pat => #wire_lit });
                encode_arms.push(quote! {
                    #pat => { __mess_encode(&()) }
                });
                decode_arms.push(quote! {
                    #wire_lit => {
                        // No payload to inspect, but keep the bytes valid.
                        let () = __mess_decode(name, data)?;
                        ::core::result::Result::Ok(#ctor)
                    }
                });
            }
        }

        fingerprint_arms.push(match &variant.fields {
            Fields::Named(n) if !n.named.is_empty() => {
                quote! { #enum_ident::#vident { .. } => #fingerprint }
            }
            Fields::Unnamed(u) if !u.unnamed.is_empty() => {
                quote! { #enum_ident::#vident(..) => #fingerprint }
            }
            Fields::Named(_) => {
                quote! { #enum_ident::#vident { .. } => #fingerprint }
            }
            Fields::Unnamed(_) => {
                quote! { #enum_ident::#vident(..) => #fingerprint }
            }
            Fields::Unit => {
                quote! { #enum_ident::#vident => #fingerprint }
            }
        });
    }

    let prefix_lit = LitStr::new(&prefix, attr.name.span());

    Ok(quote! {
        const _: () = {
            fn __mess_encode<__T: ::serde::Serialize>(
                value: &__T,
            ) -> ::core::result::Result<
                ::std::vec::Vec<u8>,
                ::mess_core::CodecError,
            > {
                ::mess_core::codec::encode_payload(
                    ::mess_core::codec::CODEC_ID_MSGPACK_NAMED,
                    value,
                )
                .map_err(|e| {
                    ::mess_core::CodecError::Encode(
                        ::std::string::ToString::to_string(&e),
                    )
                })
            }

            fn __mess_decode<__T: ::serde::de::DeserializeOwned>(
                name: &str,
                data: &[u8],
            ) -> ::core::result::Result<__T, ::mess_core::CodecError> {
                ::mess_core::codec::decode_payload(
                    ::mess_core::codec::CODEC_ID_MSGPACK_NAMED,
                    data,
                )
                .map_err(|e| ::mess_core::CodecError::Decode {
                    event_name: ::std::string::ToString::to_string(&name),
                    source: ::std::string::ToString::to_string(&e),
                })
            }

            #[automatically_derived]
            impl ::mess_core::Event for #enum_ident {
                fn name(&self) -> &'static str {
                    match self {
                        #(#name_arms),*
                    }
                }

                fn encode(
                    &self,
                ) -> ::core::result::Result<
                    ::std::vec::Vec<u8>,
                    ::mess_core::CodecError,
                > {
                    match self {
                        #(#encode_arms)*
                    }
                }

                fn decode(
                    name: &str,
                    data: &[u8],
                ) -> ::core::result::Result<Self, ::mess_core::CodecError>
                {
                    match name {
                        #(#decode_arms)*
                        other => ::core::result::Result::Err(
                            ::mess_core::CodecError::UnknownEventName(
                                ::std::string::ToString::to_string(&other),
                            ),
                        ),
                    }
                }
            }
        };

        #[automatically_derived]
        impl #enum_ident {
            /// The schema version declared via `#[event(version = N)]`.
            pub const SCHEMA_VERSION: u16 = #version;

            /// The wire-name prefix declared via `#[event(name = "...")]`.
            pub const EVENT_NAME_PREFIX: &'static str = #prefix_lit;

            /// Every wire name this event can produce, in declaration order.
            pub const EVENT_NAMES: &'static [&'static str] =
                &[#(#wire_names),*];

            /// A stable per-variant schema fingerprint: an FNV-1a hash over
            /// the wire name, schema version, and each field's name and type.
            /// An incompatible shape change to a variant changes its
            /// fingerprint, giving downstream tooling a cheap drift check.
            pub fn schema_fingerprint(&self) -> u64 {
                match self {
                    #(#fingerprint_arms),*
                }
            }
        }
    })
}

/// Parse the required `#[event(name = "...", version = N)]` attribute.
/// `version` is optional and defaults to `1`.
fn parse_event_attr(input: &DeriveInput) -> syn::Result<EventAttr> {
    let attr = input.attrs.iter().find(|a| a.path().is_ident("event"));
    let Some(attr) = attr else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(Event)] requires an `#[event(name = \"...\")]` \
             attribute naming the event's wire-name prefix, e.g. \
             `#[event(name = \"account\", version = 1)]`",
        ));
    };

    let mut name: Option<LitStr> = None;
    let mut version: Option<u16> = None;

    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("name") {
            let lit: LitStr = meta.value()?.parse()?;
            name = Some(lit);
            Ok(())
        } else if meta.path.is_ident("version") {
            let lit: LitInt = meta.value()?.parse().map_err(|_| {
                meta.error(
                    "`version` must be an integer literal, e.g. `version = 1`",
                )
            })?;
            let v: u16 = lit.base10_parse().map_err(|_| {
                syn::Error::new_spanned(
                    &lit,
                    "`version` must fit in a u16 (0..=65535)",
                )
            })?;
            version = Some(v);
            Ok(())
        } else {
            Err(meta.error(
                "unknown `#[event(...)]` key: expected `name` or `version`",
            ))
        }
    })?;

    let Some(name) = name else {
        return Err(syn::Error::new_spanned(
            attr,
            "#[event(...)] is missing the required `name = \"...\"` key, e.g. \
             `#[event(name = \"account\", version = 1)]`",
        ));
    };

    Ok(EventAttr { name, version: version.unwrap_or(1) })
}

/// Convert a `PascalCase`/`camelCase` variant identifier to `snake_case`.
fn to_snake_case(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 4);
    let mut prev_lower_or_digit = false;
    for ch in ident.chars() {
        if ch.is_uppercase() {
            if prev_lower_or_digit {
                out.push('_');
            }
            for lower in ch.to_lowercase() {
                out.push(lower);
            }
            prev_lower_or_digit = false;
        } else {
            out.push(ch);
            prev_lower_or_digit = ch.is_alphanumeric();
        }
    }
    out
}

/// Compute a per-variant schema fingerprint at expansion time.
fn variant_fingerprint(wire: &str, version: u16, fields: &Fields) -> u64 {
    let mut sig = format!("{wire}@{version}(");
    match fields {
        Fields::Named(named) => {
            for (i, f) in named.named.iter().enumerate() {
                if i > 0 {
                    sig.push(',');
                }
                let fname = f.ident.as_ref().unwrap();
                let fty = f.ty.to_token_stream().to_string();
                sig.push_str(&format!("{fname}:{fty}"));
            }
        }
        Fields::Unnamed(unnamed) => {
            for (i, f) in unnamed.unnamed.iter().enumerate() {
                if i > 0 {
                    sig.push(',');
                }
                let fty = f.ty.to_token_stream().to_string();
                sig.push_str(&format!("{i}:{fty}"));
            }
        }
        Fields::Unit => {}
    }
    sig.push(')');
    fnv1a_64(&sig)
}

/// FNV-1a 64-bit hash — small, dependency-free, and stable across builds.
fn fnv1a_64(s: &str) -> u64 {
    let mut hash: u64 = 0xCBF2_9CE4_8422_2325;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}
