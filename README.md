# chaindexer

The open source indexer and query engine for blockchain data.

# Installation

Cargo is required to build/install. The project is not on crates.io (yet!), so to install
it, you have to clone this repo, then run

```sh
cargo install --path .
```

If you don't have cargo installed, I recommend checking out `https://rustup.rs/`!

# Quickstart

Running queries and building indices both require an RPC api that you can connect to.
To specify an Ethereum RPC api, you set `ETH_RPC_API` (there is also a way to specify
more stuff via a config file, see [Config](#Config)).

It is possible to query chain data without building an index ahead of time. The one stipulation
is that queries must be highly _selective_. On-chain data is range partitioned by block number,
so queries must query a specific range of block numbers--otherwise the entire chain state
would have to be indexed (see next section for how to do that.)

For example, a valid query would be something like:

```sql
select * from ethereum.logs l
join ethereum.blocks b
on b.hash = l.block_hash
where b.number >= (ethereum_current_block() - 10) -- 10 most recent blocks
```

To run an interactive SQL shell that can query your ethereum rpc node:

```sh
ETH_RPC_API=<your rpc url> chaindexer sql
```

## Other SQL interfaces

A JDBC compatible server is coming soon (this would allow you to use this in something
like PyCharm or any other database client that accepts JDBC).

# Indexing

To query the full chain, an index for that chain must be constructed and saved either
on disk, or on a blob store (currently IPFS, and S3). An index is essentially just a file
that contains mappings for tables and block range to the location of files
containing the raw data for that table/block range. Each index is backed by a single
storage config, which specifies the location of the index file as well as where to place
the actual data partitions.

## Config

You'll probably want a config file to specify your storage layer before building an index:

```sh
chaindexer config
```

This will create a TOML file that you can edit. For example, to use S3 as your storage
layer, you can open up the config file at `~/.chaindexer/config.toml` and change the
`stores.eth` section (s3 credentials are read from the environment):

```toml
[stores.eth]
type="s3"
bucket="<your s3 bucket>"
# s3 key prefix for where all partition files will be placed
prefix="/prefix"
# the root index will be stored here. the index file contains mappings to the partition
# files which will be loaded into prefix.
# this file will also be under prefix, so if your prefix is `/prefix`, the full s3 key
# of the root index file would be `/prefix/eth.db`
filename="eth.db"
```

Each chain has its own storage configuration, which is why the storage layer for eth is
specified under `stores.eth`. If you'd rather store data on disk, the storage layer in the
default generated config file should be fine.

Now with an eth store layer specified, we can run the `index` command to start
building the ethereum index. Currently running `index` indexes the raw chain data
only (we are actively working to move some of our custom data pipelines
into this open source project).

```sh
chaindexer index --chain eth
```

Now non-selective queries can be run (if the partition index is not fully built,
it will just treat the blocks that is built for as the only blocks that exist.)

## Index time

We recognize that indexing an entire chain just to run queries is unrealistic for most users.
We are currently working to put our pre-indexed ethereum onto IPFS, so that other users
can use the query engine without needing to index the entire chain.

# Tech overview

Chain data is indexed and then persisted using Apache Parquet on various storage layers.
The query engine is leverages [DataFusion](https://github.com/apache/arrow-datafusion),
a highly flexible and extensible in-memory query engine which uses
[Apache Arrow](https://arrow.apache.org/) as its in-memory columnar data format.

Each index is a SQLite database that describes to the query engine how to access the raw
data during each table scan. For example, an ethereum index would have a row in the SQLite db
mapping from the blocks table, for block range 0 - 1,000,000 to a parquet file on S3
(or other storage layers, including your disk). That parquet file would have 1,000,000
rows, representing all the blocks in that range.

# Contributing

## Adding new chains

The code in this project is written so that it is (relatively) simple to add new chains as
data sources. For now, there isn't a great guide on this, but if you know Rust,
you should be able to follow along with our eth implementation (see `src/chains/eth/mod.rs`).

## Adding projects

TODO

## Other data sources

We want to add ways other than writing rust for users to be able to add their own data sources.
This is a work in progress, but an embedded WASM runtime that allows
flexible data defintiions is in the works.
