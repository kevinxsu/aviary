# aviary

### Compiling

To compile down to a program executable that can be directly run:
```
$ rustc src/main.rs
```

To compile down to WebAssembly (WASM), first add the general WASM target:
```
$ rustup target add wasm32-unknown-unknown
```

Then:
```
$ rustc --target wasm32-unknown-unknown --crate-type=cdylib src/main.rs
```
