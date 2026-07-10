//! The write side: the two aggregates and their commands.
//!
//! Each submodule is one aggregate — one stream family, one folded state, one
//! typed rejection, and one `Decide` impl per command. Nothing here knows
//! about storage, HTTP, or read models; that separation is the whole design.
//!
//! - [`user`] — the [`User`](user::User) aggregate and its follow graph.
//! - [`post`] — the [`Post`](post::Post) aggregate and its like set.

pub mod post;
pub mod user;
