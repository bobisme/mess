//! Compile-fail UI tests: the derives must reject missing/malformed
//! attributes and non-enum targets with helpful, spanned diagnostics.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
