//! Bin shell: the real driver lives in `driver.rs`. Under `--cfg loom` the
//! bench driver (rdtsc, rand, thread pinning) is compiled out so the loom
//! model build stays clean.

#[cfg(not(loom))]
include!("driver.rs");

#[cfg(loom)]
fn main() {
    eprintln!("bench driver is disabled under --cfg loom; run the loom test instead");
}
