# Merkle Storage
Merkle Storage with BTreeMap as a keystore
## Environment setup
**1. Install rustup command**

We recommend installing Rust through rustup.

Run the following in your terminal, then follow the onscreen instructions.

```
curl https://sh.rustup.rs -sSf | sh
```

**2. Install rust toolchain**

Rust nightly is required to build this project.
```
rustup toolchain install nightly-2020-07-12
rustup default nightly-2020-07-12
```

**3. Install required libs**

Install libs required to build sodiumoxide package:
```
sudo apt install pkg-config libsodium-dev
```
## How to build
```shell script
SODIUM_USE_PKG_CONFIG=1 cargo build
```

## How to Test


````shell script
SODIUM_USE_PKG_CONFIG=1 cargo test
````

## Benchmarking

The benchmark simulates connected clients ``c`` making ``n`` sets commands on merkle storage,
the benchmark show the number of set commands that has been committed to storage 

````shell script
cd benchmark
````

````shell script
SODIUM_USE_PKG_CONFIG=1 cargo run
````

````shell
USAGE:
    benchmark [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --cycle <CYCLE>    Cycle length, logs the memory usage at every cycle [default: 4096]
    -l, --limit <LIMIT>    Specifies the block height limit [default: 25000]
    -n, --node <NODE>      Node base url [default: http://127.0.0.1:18732]
````