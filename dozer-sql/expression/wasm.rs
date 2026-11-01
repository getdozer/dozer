// Stub implementation for WASM UDF support.
// The actual runtime integration will be added in future work.
// This file ensures that the crate compiles when the `wasm` feature is enabled.

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_test() {
        // This test only verifies that the module is linked correctly.
        assert!(true);
    }
}
