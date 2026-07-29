//! One module per reference workload. Each `run(size, ...)` returns the
//! [`crate::Metric`] rows for that workload at the given [`crate::RunSize`].
//! Every module is a size-parameterized port of an already-validated bench
//! entry point; see the module doc on each for its source.

mod common;

pub mod buffered_append;
pub mod durable_append;
pub mod engine;
pub mod fold_chain;
pub mod load_verified;
pub mod reader_contention;
pub mod recovery;
pub mod sealed_payload;
pub mod sealed_pointer;
