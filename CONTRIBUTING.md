# Contributing

## Adding new chains

The code in this project is written so that it is (relatively) simple to add new chains as
data sources. For now, there isn't a great guide on this, but if you know Rust,
you should be able to follow along with our eth implementation--see `src/chains/eth/mod.rs`--
additionally if is also a chain that uses an eth compatible RPC,
you can also re-use our ethereum rpc api impl in that module.

Specifically, a chain must implement trait `ChainDef` and all entities of a chain
must implement `EntityDef` (see `EthEntity` for example of implementing that trait).
Once the chain is implemented, you can add it as variant in the `Chain` enum (found in `src/chains/mod.rs`).

## Adding projects

TODO

## Other data sources

We want to add ways other than writing rust for users to be able to add their own data sources.
This is a work in progress, but an embedded WASM runtime that allows
flexible data defintiions is in the works.

# Dev environment

This is a pretty basic rust project, everything is very standard in terms of dev environment.
There are a few extra things that must be present for running _some_ tests. But these
aren't always necessary, depending on what tests you are trying to run.

## Testing

Some of our unit tests require other services running locally.

To run all tests, run

```sh
TEST_INTEGRATION=1 cargo test
```

When `TEST_INTEGRATION` flag is set in environment, tests will fail if the required
env vars are not found in environment. When it's not set, integration tests are simply skipped.

Here are the current things required for running all tests when `TEST_INTEGRATION=1`

- Ethereum rpc node
  - `TEST_ETH_RPC_URL`
- IPFS kubo daemon running with access
  - `TEST_IPFS_API`
  - `TEST_IPFS_GATEWAY`
- Localstack running for testing s3:

```sh
pip3 install localstack
pip3 install awslocal
localstack start
awslocal s3api create-bucket --bucket testbucket
```

Integration tests are Sample .env file:

```
RUST_LOG=info
TEST_ETH_RPC_URL=http://127.0.0.1:8545
TEST_IPFS_API=http://127.0.0.1:5001
TEST_IPFS_GATEWAY=http://127.0.0.1:8080
TEST_S3_LOCALSTACK=http://127.0.0.1:4566
TEST_S3_BUCKET=testbucket
```
