//! `#[derive(Aggregate)]` expansion.
//!
//! Thin plumbing only: the derive emits `impl mess_core::Aggregate` with the
//! event type from `#[aggregate(event = Type)]` and forwards the trait
//! `apply` to an inherent method the user writes by hand — `apply` *is* the
//! domain logic and never gets generated.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Type};

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let event_ty = parse_event_type(&input)?;
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
    })
}

/// Parse the required `#[aggregate(event = Type)]` attribute.
fn parse_event_type(input: &DeriveInput) -> syn::Result<Type> {
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
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("event") {
            event_ty = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error(
                "unknown `#[aggregate(...)]` key: expected `event`",
            ))
        }
    })?;

    event_ty.ok_or_else(|| {
        syn::Error::new_spanned(
            attr,
            "#[aggregate(...)] is missing the required `event = Type` key, \
             e.g. `#[aggregate(event = AccountEvent)]`",
        )
    })
}
