//! mess v1: proc-macros — `#[derive(Event)]` and `#[derive(Aggregate)]`.
//!
//! These derives absorb exactly the mechanical boilerplate that the
//! `spikes/dx_api` bank-account example hand-writes (~30 lines/enum): the
//! `name()` match, the encode/decode plumbing, and the per-variant name
//! dispatch. They expand to hand-writable impls of the traits in
//! [`mess_core`], so nothing here is load-bearing beyond code generation.
//!
//! # `#[derive(Event)]`
//!
//! ```ignore
//! #[derive(Event)]
//! #[event(name = "account", version = 1)]
//! enum AccountEvent {
//!     Opened { owner: String },
//!     Deposited { amount: i64 },
//!     Withdrawn { amount: i64 },
//! }
//! ```
//!
//! generates `impl mess_core::Event for AccountEvent`, mapping each variant
//! to a dotted, `snake_case` wire name (`Opened` -> `"account.opened"`) and
//! routing `encode`/`decode` through mess-core's codec layer under
//! `codec_id 1` (msgpack-named).
//!
//! **Wire names are stable under variant reorder.** `decode` dispatches on
//! the *stored name string*, never on a variant index or serde's internal
//! enum tagging: the payload carries only a variant's own fields, and the
//! name selects which variant to rebuild. Reordering the variants — or
//! decoding bytes written by an enum whose variants were declared in a
//! different order — cannot silently swap the decoded variant.
//!
//! The deriving crate must depend on both `mess-core` and `serde` (the
//! generated code names `::mess_core` and `::serde`).
//!
//! # `#[derive(Aggregate)]`
//!
//! Thin by design: `apply` *is* the domain logic and stays hand-written as
//! an inherent method; the derive only fills the mechanical trait plumbing.
//!
//! ```ignore
//! #[derive(Default, Aggregate)]
//! #[aggregate(event = AccountEvent)]
//! struct Account { open: bool, balance: i64 }
//!
//! impl Account {
//!     fn apply(&mut self, event: &AccountEvent) { /* domain logic */ }
//! }
//! ```
//!
//! generates `impl mess_core::Aggregate for Account { type Event =
//! AccountEvent; fn apply(..) { Account::apply(self, event) } }`, forwarding
//! to the inherent `apply`.

mod aggregate;
mod event;

use proc_macro::TokenStream;

/// Derive [`mess_core::Event`] for an enum. See the crate docs for the
/// `#[event(name = "...", version = N)]` attribute and the wire-name
/// contract.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    event::expand(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Derive [`mess_core::Aggregate`] for a struct, forwarding `apply` to an
/// inherent method of the same signature. See the crate docs for the
/// `#[aggregate(event = Type)]` attribute.
#[proc_macro_derive(Aggregate, attributes(aggregate))]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    aggregate::expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
