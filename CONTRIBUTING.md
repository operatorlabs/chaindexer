# Tests

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
