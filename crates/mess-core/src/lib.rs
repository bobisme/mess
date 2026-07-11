//! mess v1: the product's core event-sourcing vocabulary.
//!
//! This crate is the DX-first heart of mess, ported from the proven
//! plain-trait spike in `spikes/dx_api`. It defines the traits an application
//! author implements — [`Event`], [`Aggregate`], and [`Decide`] — and the
//! error taxonomy those traits and their callers speak in ([`CodecError`],
//! [`CommandError`]).
//!
//! # Design contract
//!
//! - [`Aggregate::apply`] is **infallible**: replaying committed history never
//!   fails.
//! - [`Decide::decide`] returns a **typed rejection** ([`Decide::Rejection`], a
//!   `std::error::Error`) instead of the spike's stringly-typed `DomainError`.
//! - No proc macros here. The `#[derive(Event)]` / `#[derive(Aggregate)]` layer
//!   (bn-hy7) lands separately and expands to hand-writable impls of *these*
//!   traits, so every trait surface is kept small and mechanical.
//! - **Purity.** `mess-core` depends on no backend crate; a store supplies the
//!   `S` type parameter of [`CommandError`], never the other way around.
//!
//! The on-disk log format and registry (`docs/spec/01-log-format.md`,
//! `docs/spec/04-registry.md`) are a backend concern; this crate is the
//! application-facing seam that sits above them.

pub mod aggregate;
pub mod error;
pub mod event;

pub use aggregate::{Actor, Aggregate, Decide};
pub use error::{BoxedStoreError, CodecError, CommandError, SharedStoreError};
pub use event::Event;

pub mod codec;
