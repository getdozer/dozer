# Cross compilation

We use [cross](https://github.com/cross-rs/cross) to work around [bug](https://github.com/rust-lang/rust-bindgen/issues/1229) in `bind-gen`.

To test cross compilation locally:

```bash
cargo install cross
cross build --target ${target} --bin dozer
```
