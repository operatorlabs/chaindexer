# goals

### immediate

- Provide an open source query engine on top of blockchain data shared thru IPFS.
  - By making it open source and extensible, incentivize stakeholders in specific
    blockchains to contribute code and pre-populate raw data in IPFS by increasing
    reach/usage of their chain

### longer term

- use subgraphs to automatically infer views for protocols/projects. esssentially
  providing a bridge between The Graph's graphql style queries (not as good for
  analytics) and Dune style SQL analytics (good for analytics but some semantic data
  is lost since they simply generate decoded projects using the contract addresses
  and ABIs.)
- Given raw chain data, provide extensible ways for protocols to define
  custom transform/indexing on it. Each protocol will be able to define an adapter
  that maps raw contract addresses into queryable data. Basically like the Graph
  but for analytical queries.

# Chain data

Each chain can define a way to bootstrap its data when the program is booted up.

# Config

- Chain-specific config
  - RPC url, batch size, etc. etc.
- Shared config
  - Partition discovery config

## Partition discovery conf

- `partition_mapping_location` (nullable)
  - Where to get the mapping from

### Partition

    - data location
        - file path, s3 key, ipfs CID
    - file format--only parquet for now
    - lower/upper

# U256

Since there is no uint256 arrow type, we store them as strings. This way,
all info is preserved in cases where every digit is needed (for example when they
are used as identifiers). In cases where they are treated as numbers,
they can be cast at runtime into decimals.

### Todos

Support Decimal256 for quantities (such as gas limit) from ethereum RPC instead of Float64 which
loses some precision.
