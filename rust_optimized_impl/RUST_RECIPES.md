This document describes some common Rust commands.

###### Install Rust on your machine

https://www.rust-lang.org/tools/install describes
```console
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

###### Compilation of your current package
Compile unoptimized and with debuginfo
```console
cargo build
```

###### Compile for a release: optimized and without debuginfo. (takes longer to compile)
```console
cargo build --release
```

###### Where can I find my binaries?
target/debug
target/release


