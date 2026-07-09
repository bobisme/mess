//! `#[derive(Aggregate)]` expansion.
//!
//! Thin plumbing only: the derive emits `impl mess_core::Aggregate` with the
//! event type from `#[aggregate(event = Type)]` and forwards the trait
//! `apply` to an inherent method the user writes by hand — `apply` *is* the
//! domain logic and never gets generated.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, Type};

/// Parsed `#[aggregate(event = Type, fold_version = N)]` attribute.
struct AggregateAttr {
    event_ty: Type,
    /// The `u32` semantic fold version (D4 / spec `05-fold-certificates.md`
    /// §9). Optional; defaults to `1`, matching `#[event(version = N)]`.
    fold_version: u32,
}

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let attr = parse_aggregate_attr(&input)?;
    let event_ty = &attr.event_ty;
    let fold_version = attr.fold_version;
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::mess_core::Aggregate for #ident #ty_generics
            #where_clause
        {
            type Event = #event_ty;

            fn apply(&mut self, event: &#event_ty) {
                // Forwards to the hand-written inherent `apply`. The
                // type-qualified path resolves to the inherent method
                // (inherent methods shadow trait methods), so this is not a
                // recursive call into the trait impl.
                #ident::apply(self, event)
            }
        }

        #[automatically_derived]
        impl #impl_generics #ident #ty_generics #where_clause {
            /// Explicit, human-bumped semantic version of this aggregate's
            /// fold, declared via `#[aggregate(fold_version = N)]`
            /// (spec `05-fold-certificates.md` §9). Snapshots carry this
            /// value; the generated fold-drift golden test asserts it stays
            /// pinned so an `apply` semantic change forces a deliberate bump.
            pub const FOLD_VERSION: u32 = #fold_version;
        }
    })
}

/// Parse `#[aggregate(event = Type, fold_version = N)]`. `event` is required;
/// `fold_version` is optional and defaults to `1`.
fn parse_aggregate_attr(input: &DeriveInput) -> syn::Result<AggregateAttr> {
    let attr = input.attrs.iter().find(|a| a.path().is_ident("aggregate"));
    let Some(attr) = attr else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(Aggregate)] requires an `#[aggregate(event = Type)]` \
             attribute naming the event type this aggregate folds, e.g. \
             `#[aggregate(event = AccountEvent)]`",
        ));
    };

    let mut event_ty: Option<Type> = None;
    let mut fold_version: Option<u32> = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("event") {
            event_ty = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("fold_version") {
            let lit: LitInt = meta.value()?.parse().map_err(|_| {
                meta.error(
                    "`fold_version` must be an integer literal, e.g. \
                     `fold_version = 1`",
                )
            })?;
            let v: u32 = lit.base10_parse().map_err(|_| {
                syn::Error::new_spanned(
                    &lit,
                    "`fold_version` must fit in a u32 (0..=4294967295)",
                )
            })?;
            fold_version = Some(v);
            Ok(())
        } else {
            Err(meta.error(
                "unknown `#[aggregate(...)]` key: expected `event` or \
                 `fold_version`",
            ))
        }
    })?;

    let Some(event_ty) = event_ty else {
        return Err(syn::Error::new_spanned(
            attr,
            "#[aggregate(...)] is missing the required `event = Type` key, \
             e.g. `#[aggregate(event = AccountEvent)]`",
        ));
    };

    Ok(AggregateAttr { event_ty, fold_version: fold_version.unwrap_or(1) })
}
