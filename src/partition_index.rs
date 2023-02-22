//! Data structures representing relations and locations of queryable data.
//!
//! The primary data structures are:
//! - [`ChainPartitionIndex`]: represents a chain and all its tables. This structure defines
//! a chain and all its tables. For each table in the chain, there is a key value mapping
//! from block number to partition location. The key represents the lower bound of blocks
//! whose data is stored in each partition.
//!
//! - [`TablePartitionIndex`]: a single table and its partitions (ranged partition
//!  over block number.)
//!
//! All indices are backed by SQLite db to provide a persistence format and
//! an efficient interface for querying data.

// TODO: enforce that partition index uses the same StorageApi for all its partitions
use crate::{
    storage::{Location, Persistable, StorageApi},
    table_api::{BlockNumSet, TableApi},
    util::SwappableMemBuf,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::{
    arrow::{
        datatypes::Schema,
        ipc::{reader::FileReader, writer::FileWriter},
        record_batch::RecordBatch,
    },
    physical_plan::Statistics,
};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use log::{debug, info, warn};
use object_store::ObjectMeta;
use rusqlite::{params, Connection, DatabaseName};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use std::{io, num::NonZeroUsize};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::{
    sync::{mpsc, Mutex, OwnedMutexGuard, RwLock},
    task::spawn_blocking,
};

/// The "unit" data structure of the index. Represents a single partition of data
/// for the given block range: from `lower_bound` to `lower_bound + blocks_per_partition`
/// (except for potentially the most recent partition, in which case `incomplete` will
/// be set to `true`.)
///
/// The actual data is not stored in this object,
/// instead it holds a `location` which points to it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlockPartition {
    /// table this partition belongs to
    pub table: String,
    /// Lower bound of this partition. Also used as ID
    pub lower_bound: u64,
    /// data location
    pub location: Location,
    pub created_at: DateTime<Utc>,
    /// optional byte count stat
    pub byte_count: Option<u64>,
    /// optional row count stat
    pub row_count: Option<u64>,
    /// flag to mark if the partition does not include all blocks worth of data.
    pub incomplete: bool,
}
impl Default for BlockPartition {
    fn default() -> Self {
        Self {
            table: "table".to_owned(),
            lower_bound: 0,
            location: Location::new("memory", None, ""),
            byte_count: None,
            row_count: None,
            incomplete: false,
            created_at: Utc::now(),
        }
    }
}
impl BlockPartition {
    pub fn as_object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            location: self.location.path(),
            last_modified: self.created_at,
            // TODO: what should we do if byte_count is null??
            size: self.byte_count.unwrap_or(0) as usize,
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("error occurred during sqlite operation")]
    SqliteError(#[from] rusqlite::Error),
    #[error("unexpected error occurred due to invalid database content")]
    InvalidDatabaseContent(#[from] anyhow::Error),
    #[error("got invalid input: {0}")]
    InvalidInput(String),
    #[error("got error while reading/writing to disk")]
    DiskIoError(#[from] std::io::Error),
    #[error("detected deadlock (timed out after {0}) while attempting to write to index")]
    DeadlockTimeout(tokio::time::error::Elapsed),
    // #[error("failed to build new partition")]
    // PartitionBuildError(#[from] anyhow::Error),
}

/// Data structure for storing a table and its partitions ([`BlockPartition`])
/// (ranged partition over block number.)
///
/// Exposes a key-value mapping from lower bounds
/// to partitions. Designed to be used in isolation and as a child of a chain data.
///
/// **NOTE**: This struct does not hold the actual [`BlockPartition`]s and is just a reference to the underlying
/// sqlite db so it can cheaply be cloned.
#[derive(Debug, Clone)]
pub struct TablePartitionIndex {
    /// Name of the table and also identifies tables in sqlite db
    name: String,
    /// how many blocks worth of data goes into each partition
    blocks_per_partition: u64,
    blocknum_column: String,
    /// shared sqlite db
    conn: Conn,
    /// arrow schema of the partitions in this table.
    /// when writing to this tables index, need to check that the schemas match.
    ///
    /// schema is currently optional but we probably want it to be required.
    schema: Option<Arc<Schema>>,
    /// table stats for datafusion query planner
    /// this number is approximate, not exact
    row_count: Arc<parking_lot::Mutex<usize>>,
    /// table stats for datafusion query planner: total byte count across all partitions.
    /// this number is approximate, not exact
    byte_count: Arc<parking_lot::Mutex<usize>>,
}

impl TablePartitionIndex {
    pub async fn try_new(
        name: &str,
        blocknum_column: &str,
        blocks_per_partition: u64,
    ) -> Result<Self> {
        let tempfile = Arc::new(RwLock::new(Some(
            NamedTempFile::new().expect("failed to create tempfile on disk"),
        )));
        let conn_mutex = Arc::new(Mutex::new(()));
        Self::init(
            Conn {
                db_path: Arc::new(RwLock::new(None)),
                tempfile,
                conn_mutex,
            },
            name,
            blocknum_column,
            blocks_per_partition,
            None,
        )
        .await
    }

    pub fn stats(&self) -> Statistics {
        let rows = self.row_count.lock();
        let bytes = self.byte_count.lock();
        Statistics {
            num_rows: Some((*rows).to_owned()),
            total_byte_size: Some((*bytes).to_owned()),
            column_statistics: None,
            is_exact: false,
        }
    }

    /// In order of their lower bounds, return a stream of block partitions.
    /// Return `per_chunk` block partitions at a time.
    pub fn stream_block_partitions(
        &self,
        per_chunk: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<BlockPartition>>> + Send + 'static>> {
        enum State {
            Iter(usize),
            Errored,
        }
        async fn stream_iter(
            conn: Conn,
            table_name: String,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<BlockPartition>> {
            let conn = conn.acquire_conn().await?;
            spawn_blocking(move || {
                let mut stmt = conn
                    .prepare(
                        format!(
                            "select data from {table_name}_partitions \
                             order by lower_bound \
                             limit {limit} offset {offset}",
                        )
                        .as_str(),
                    )
                    .map_err(IndexError::SqliteError)?;
                let rows = stmt
                    .query_map::<String, _, _>((), |row| row.get(0))
                    .map_err(IndexError::SqliteError)?;
                rows.into_iter()
                    .map(|s| match s {
                        Ok(v) => serde_json::from_str::<BlockPartition>(&v).map_err(|e| {
                            anyhow!(e).context("failed to parse partition json data in sqlite db")
                        }),
                        Err(err) => Err(anyhow!(err)),
                    })
                    .collect()
            })
            .await
            .expect("blocking task should never panic")
        }
        let conn_ = self.conn.clone();
        let name_ = self.name.to_owned();

        Box::pin(futures::stream::unfold(State::Iter(0), move |state| {
            let conn_clone = conn_.clone();
            let name = name_.clone();
            async move {
                match state {
                    State::Iter(offset) => {
                        match stream_iter(conn_clone.clone(), name.clone(), offset, per_chunk).await
                        {
                            Ok(parts) => {
                                if parts.is_empty() {
                                    None
                                } else {
                                    Some((Ok(parts), State::Iter(offset + per_chunk)))
                                }
                            }
                            Err(err) => Some((Err(err), State::Errored)),
                        }
                    }
                    State::Errored => None,
                }
            }
        }))
    }

    /// Creates a new table in the given SQLite db. This is for adding tables to from a chain,
    /// but also  for testing. Eventually we want to be able to initialize data indices
    /// for tables outside of the context of a chain.
    async fn init(
        conn: Conn,
        name: &str,
        blocknum_column: &str,
        blocks_per_partition: u64,
        schema: Option<Arc<Schema>>,
    ) -> Result<Self> {
        let partition_ddl = Self::partition_ddl(name);
        let table_ddl = ChainPartitionIndex::ddl().0;
        let this = Self {
            name: name.to_owned(),
            blocks_per_partition,
            blocknum_column: blocknum_column.to_owned(),
            conn,
            schema: schema.clone(),
            byte_count: Arc::new(parking_lot::Mutex::new(0)),
            row_count: Arc::new(parking_lot::Mutex::new(0)),
        };
        let conn = this.acquire_conn().await?;
        let blocknum_column = blocknum_column.to_owned();
        let name = name.to_owned();
        let schema = schema.clone();
        spawn_blocking(move || {
            conn.execute(table_ddl, ())
                .map_err(IndexError::SqliteError)?;
            conn.execute(&partition_ddl, ())
                .map_err(IndexError::SqliteError)?;
            let schemablob = schema.map(|v| {
                let bytes = schema_to_bytes(v).unwrap();
                bytes.to_vec()
            });
            conn.execute(
                "INSERT INTO all_tables (
                    name, 
                    blocknum_column, 
                    blocks_per_partition, 
                    arrow_schema_blob,
                    total_row_count,
                    total_byte_count
                )
                 VALUES (?1, ?2, ?3, ?4, 0, 0);",
                (&name, blocknum_column, blocks_per_partition, schemablob),
            )?;
            Ok(()) as Result<_, IndexError>
        })
        .await
        .expect("blocking task should never panic")?;
        Ok(this)
    }

    /// Get the last partition in the table (the partition with the highest id/lower bound.)
    pub async fn last_partition(&self) -> Result<Option<BlockPartition>> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        Ok(spawn_blocking(move || {
            conn.query_row(
                format!(
                    "SELECT data FROM {name}_partitions
                     ORDER BY lower_bound DESC
                     LIMIT 1;"
                )
                .as_str(),
                (),
                |row| {
                    let data: String = row.get(0)?;
                    // error will be discared anways.
                    serde_json::from_str(&data).map_err(|_| rusqlite::Error::InvalidQuery)
                },
            )
            .ok()
        })
        .await
        .expect("blocking task should never panic"))
    }
    /// Get the last partition in the table (the partition with the highest id/lower bound.)
    pub async fn first_partition(&self) -> Result<Option<BlockPartition>> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        Ok(spawn_blocking(move || {
            conn.query_row(
                format!(
                    "SELECT data FROM {name}_partitions
                     ORDER BY lower_bound
                     LIMIT 1;"
                )
                .as_str(),
                (),
                |row| {
                    let data: String = row.get(0)?;
                    // error will be discared anways.
                    serde_json::from_str(&data).map_err(|_| rusqlite::Error::InvalidQuery)
                },
            )
            .ok()
        })
        .await
        .expect("blocking task should never panic"))
    }
    /// Total amount of partitions for this table.
    pub async fn num_partitions(&self) -> Result<usize> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        spawn_blocking(move || {
            let res: usize = conn.query_row(
                format!("SELECT count(*) FROM {name}_partitions").as_str(),
                (),
                |row| row.get(0),
            )?;
            Ok(res)
        })
        .await
        .expect("blocking task should never panic")
    }

    fn partition_ddl(name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {name}_partitions (
                table_name TEXT NOT NULL,
                lower_bound INTEGER NOT NULL,
                schema BLOB,
                -- json encoded data so we can add stats and other metadata later
                data TEXT NOT NULL,
                PRIMARY KEY (table_name, lower_bound),
                FOREIGN KEY (table_name) REFERENCES all_tables(name)
            );"
        )
    }

    /// validates that the underlying sqlite database is in a valid state
    async fn is_valid(&self) -> Result<bool> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        Ok(spawn_blocking(move || {
            // check that partitions table exists
            if conn
                .execute(
                    format!("select * from {name}_partitions limit 1").as_str(),
                    (),
                )
                .is_err()
            {
                return false;
            }
            // check that main table exists and the metadata is correct
            if let Ok(n) = conn.query_row::<String, _, _>(
                "SELECT name
                 FROM all_tables
                 WHERE name = ?1;",
                params![name],
                |row| row.get(0),
            ) {
                n == name
            } else {
                false
            }
        })
        .await
        .expect("failed to join tokio blocking tak"))
    }

    /// Name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn blocknum_column(&self) -> &str {
        &self.blocknum_column
    }
    pub fn blocks_per_partition(&self) -> u64 {
        self.blocks_per_partition
    }

    /// Return all partition ids contained in this table.
    /// Partition ids are equivalent to the lower bound of the blocknumbers
    /// in a given partition.
    pub async fn partition_ids(&self) -> Result<Vec<u64>> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        spawn_blocking(move || {
            let mut stmt = conn
                .prepare(
                    format!(
                        "select lower_bound from {name}_partitions 
                         order by lower_bound",
                    )
                    .as_str(),
                )
                .map_err(IndexError::SqliteError)?;
            let res = stmt
                .query_map::<u64, _, _>((), |row| row.get(0))
                .map_err(IndexError::SqliteError)?;
            Ok(res.map(|v| v.unwrap()).collect_vec())
        })
        .await
        .expect("blocking task should never panic")
    }

    /// Write a new partition to this table. Errors if partition is invalid.
    /// Partition can be invalid if:
    /// - partition.table does not match this table
    /// - partition.lower_bound is not a multiple of blocks_per_partition
    pub async fn add_partition(&self, partition: BlockPartition) -> Result<()> {
        if partition.table != self.name {
            bail!("partition does not belong to this table");
        }
        if partition.lower_bound % self.blocks_per_partition != 0 {
            bail!("partition lower bound is not a multiple of blocks per partition")
        }
        let mut conn = self.acquire_conn().await?;
        let (rowdelta, bytedelta) = spawn_blocking(move || {
            let tx = conn.transaction()?;
            let v = Self::write_partition_helper(tx, partition)?;
            Ok(v)
        })
        .await
        .expect("blocking task should never panic")
        .map_err(IndexError::SqliteError)?;

        let mut rowguard = self.row_count.lock();
        let mut byteguard = self.byte_count.lock();
        *rowguard += rowdelta;
        *byteguard += bytedelta;
        Ok(())
    }

    fn write_partition_helper(
        conn: rusqlite::Transaction,
        partition: BlockPartition,
    ) -> Result<(usize, usize), rusqlite::Error> {
        let name = &partition.table;
        let existing = conn.query_row(
            format!(
                "select data from {name}_partitions 
                 where lower_bound=?1"
            )
            .as_str(),
            params![&partition.lower_bound],
            |row| {
                let s: String = row.get(0)?;
                let part: BlockPartition =
                    serde_json::from_str(&s).map_err(|_| rusqlite::Error::InvalidQuery)?;
                Ok(part)
            },
        );
        let had_existing = existing.is_ok();
        let (erows, ebytes) = match existing {
            Ok(b) => (b.row_count.unwrap_or(0), b.byte_count.unwrap_or(0)),
            Err(_) => (0, 0),
        };
        // remove existing partition
        if had_existing {
            conn.execute(
                format!("DELETE FROM {name}_partitions WHERE lower_bound=?1").as_str(),
                params![&partition.lower_bound],
            )?;
        }
        conn.execute(
            format!(
                "INSERT INTO {name}_partitions (table_name, lower_bound, data)
                 VALUES (?1, ?2, ?3);",
            )
            .as_str(),
            params![
                &partition.table,
                &partition.lower_bound,
                serde_json::to_string(&partition).unwrap(),
            ],
        )?;
        // update stats, us delta between new partition and previous (or 0)
        let rowsdelta = partition.row_count.unwrap_or(0) - erows;
        let bytesdelta = partition.byte_count.unwrap_or(0) - ebytes;
        conn.execute(
            "UPDATE all_tables SET
             total_row_count=total_row_count + ?1,
             total_byte_count=total_byte_count + ?2
             WHERE name = ?3",
            params![rowsdelta, bytesdelta, name],
        )?;
        conn.commit()?;
        Ok((rowsdelta as usize, bytesdelta as usize))
    }

    /// Get the partition given its ID/lower bound.
    pub async fn get_partition(&self, id: u64) -> Result<Option<BlockPartition>> {
        let conn = self.acquire_conn().await?;
        let name = self.name.clone();
        spawn_blocking(move || {
            if let Ok(val) = conn.query_row::<String, _, _>(
                format!("select data from {name}_partitions where lower_bound=?1;",).as_str(),
                params![id],
                |row| row.get(0),
            ) {
                serde_json::from_str(&val).map_err(|e| {
                    anyhow!(IndexError::InvalidDatabaseContent(e.into()))
                        .context("failed to convert json in sqlite db to a valid block partition")
                })
            } else {
                Ok(None)
            }
        })
        .await
        .expect("blocking task should never panic")
    }
    /// Add all partitions from `src` table index into this one.
    ///
    /// This will return an error if there are overlapping partition ids, or if the source
    /// table's name or partition size doesnt match up.
    pub async fn merge_block_partitions(&self, src: &TablePartitionIndex) -> Result<()> {
        if src.name != self.name {
            bail!(
                "source table for merge is invalid: name '{}' does not match this ('{}')",
                src.name,
                self.name
            )
        }
        if src.blocks_per_partition != self.blocks_per_partition {
            bail!(
                "source table for merge is invalid: blocks_per_partition {} does not match this ({})",
                src.blocks_per_partition,
                self.blocks_per_partition
            )
        }
        let this_ids: HashSet<u64> = self.partition_ids().await?.into_iter().collect();
        let src_ids: HashSet<u64> = src.partition_ids().await?.into_iter().collect();
        let inter = this_ids.intersection(&src_ids).collect_vec();
        if !inter.is_empty() {
            bail!(
                "source table for merge is invalid: following ids overlap: {}",
                inter.iter().join(", ")
            )
        }
        src.stream_block_partitions(10)
            .try_for_each(|parts| async {
                // mutex means each is done in series anyways
                for part in parts {
                    self.add_partition(part).await?;
                }
                Ok(())
            })
            .await?;
        Ok(())
    }
}
/// Chain partition index is essentially just a group of [`TablePartitionIndex`]s all
/// backed by a shared sqlite store.
///
/// Each instance of this struct represents the tables for a single chain and is backed
/// by a single sqlite db.
#[derive(Debug, Clone)]
pub struct ChainPartitionIndex {
    /// identifies the chain that this index is for
    id: String,
    blocks_per_partition: u64,
    /// shared sqlite db
    conn: Conn,
}

#[async_trait]
impl Persistable for ChainPartitionIndex {
    async fn to_bytes(&self) -> Result<Bytes> {
        let conn = self.acquire_conn().await?;

        let tempfile = spawn_blocking(move || {
            let tempfile =
                NamedTempFile::new().expect("should always be able to create a named temp file");
            let path = tempfile.path();
            // write it to disk and read
            conn.backup(DatabaseName::Main, path, None)?;
            Result::<_, rusqlite::Error>::Ok(tempfile)
        })
        .await
        .expect("tokio join failed")
        .map_err(IndexError::SqliteError)?;
        let bytes = tokio::fs::read(tempfile.path())
            .await
            .map_err(IndexError::DiskIoError)?;
        Ok(bytes.into())
    }

    async fn from_bytes(bytes: &Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        let v = bytes.clone();
        spawn_blocking(move || {
            let tf =
                NamedTempFile::new().expect("should always be able to create a named temp file");
            std::fs::write(tf.path(), v).map_err(IndexError::DiskIoError)?;
            let conn = Connection::open(tf.path()).map_err(IndexError::SqliteError)?;
            // look up the chain name in the underlying db
            // chain_data table should always have only one row since each sqlite db
            // should contain only a single chain!
            let (chain, blocks_per_partition) = conn
                .query_row::<(String, u64), _, _>(
                    "SELECT chain, blocks_per_partition FROM chain_data",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(IndexError::SqliteError)?;
            Ok(Self {
                id: chain,
                blocks_per_partition,
                conn: Conn {
                    conn_mutex: Arc::new(Mutex::new(())),
                    tempfile: Arc::new(RwLock::new(Some(tf))),
                    db_path: Arc::new(RwLock::new(None)),
                },
            })
        })
        .await
        .expect("blocking task should not panic")
    }
}
#[derive(Debug, Clone, Default)]
pub struct CreatePartitionOpts {
    /// for fetching data using a [`TableApi`], dictates how many
    /// blocks worth of data should be yielded each iteration (of the stream).
    /// Each iteration yields a record batch so this also dictates the size of the individual
    /// record batches that make up the table.
    pub blocks_per_batch: Option<u64>,
    // optional block to end on (otherwise it builds the entire partition)
    pub end: Option<u64>,
    /// When construcitng the parquet files, how many batches should be written
    /// to each row group. The last rowgroup may end having less than this number.
    pub batches_per_row_group: Option<usize>,
}
/// type alias for a row from `all_tables`  in the sqlite db
type TableRow = (String, String, u64, Option<Vec<u8>>, usize, usize);
/// this value is pretty arbitary. need to look into this more
const DEFAULT_BATCHES_PER_PARTITION: u64 = 10;
impl ChainPartitionIndex {
    /// Create a new partition with id/lowerbound `partition_id` using `table` to fetch
    /// data and `store` to persist it.
    ///
    /// `opts` are optional args (see [`CreatePartitionOpts`] docstring)
    pub async fn create_partition(
        &self,
        partition_id: u64,
        table: Arc<dyn TableApi>,
        store: &StorageApi<ChainPartitionIndex>,
        opts: CreatePartitionOpts,
    ) -> Result<BlockPartition> {
        let start = partition_id;
        let blocks_per_partition = self.blocks_per_partition;
        let end = opts.end.unwrap_or(start + blocks_per_partition);
        let blocks_per_batch = opts
            .blocks_per_batch
            .unwrap_or_else(|| u64::min(blocks_per_partition / DEFAULT_BATCHES_PER_PARTITION, 1));
        let batches_per_rowgroup = opts.batches_per_row_group.unwrap_or(1);
        let table_idx = self
            .get_table(table.name())
            .await?
            .ok_or_else(|| anyhow!("no table named {} exists in this index", table.name()))?;
        // if schema is stored, validate it now
        if let Some(schema) = table_idx.schema {
            if schema != table.schema().into() {
                bail!(
                    "existing schema stored in index for table '{}' does not match new one",
                    table.name()
                )
            }
        }
        let partition_id = format!("{}/{}/{start}.parquet", self.chain_id(), table.name());
        // spawn channels to get updates on block number and rows processed counts
        let (tx, mut rx) = mpsc::unbounded_channel::<(u64, u64)>();
        info!(
            "{} starting new iteration from {start} to {end}",
            table.name(),
        );
        let in_progress = end - start < blocks_per_partition;
        let name = table.name().to_owned();
        let name_clone = name.clone();
        let chan_updates = tokio::task::spawn(async move {
            // get channel updates to track progress and total row count
            let mut total_row_count = 0;
            while let Some((row_count, blocknum)) = rx.recv().await {
                debug!("{name_clone} processed {row_count} for blocknum: {blocknum}");
                total_row_count += row_count;
            }
            total_row_count
        });
        let stream = table
            .stream_batches(&BlockNumSet::Range(start, end), blocks_per_batch, Some(tx))
            .into_parquet_bytes(Some(NonZeroUsize::new(batches_per_rowgroup).unwrap()));
        let write_result = store.write_partition(&partition_id, stream).await?;
        let total_rowcount = tokio::time::timeout(Duration::from_millis(100), chan_updates)
            .await
            .map_err(|to| anyhow!(to).context("counting channel was not closed"))?
            .expect("task join failed?");
        let partition = BlockPartition {
            table: name,
            lower_bound: start,
            location: write_result.loc,
            byte_count: write_result.byte_count,
            row_count: Some(total_rowcount),
            incomplete: in_progress,
            created_at: Utc::now(),
        };
        self.add_partition(partition.clone()).await?;
        // save partition to index
        Ok(partition)
    }

    pub async fn try_new(chain: &str, blocks_per_partition: u64) -> Result<Self> {
        let tempfile = Arc::new(RwLock::new(Some(
            NamedTempFile::new().expect("should be able to create a named temporary file"),
        )));
        let conn_mutex = Arc::new(Mutex::new(()));
        let this = Self {
            id: chain.to_owned(),
            blocks_per_partition,
            conn: Conn {
                tempfile,
                db_path: Arc::new(RwLock::new(None)),
                conn_mutex,
            },
        };
        let conn = this.acquire_conn().await?;
        let chainclone = chain.to_owned();
        spawn_blocking(move || {
            let (ddl1, ddl2) = Self::ddl();
            conn.execute(ddl1, ())?;
            conn.execute(ddl2, ())?;
            conn.execute(
                "INSERT INTO chain_data(chain, blocks_per_partition, metadata)
                 VALUES (?1, ?2, '{}')",
                params![chainclone, blocks_per_partition],
            )?;
            Result::<_, rusqlite::Error>::Ok(())
        })
        .await
        .expect("blocking task shouldnt panic")?;
        Ok(this)
    }

    pub fn blocks_per_partition(&self) -> u64 {
        self.blocks_per_partition
    }

    /// For table named `table` get partition with id `partition_id` (or None if it doesnt exist).
    pub async fn get_partition(
        &self,
        table: &str,
        partition_id: u64,
    ) -> Result<Option<BlockPartition>> {
        let conn = self.acquire_conn().await?;
        let sql = format!("select data from {table}_partitions where lower_bound={partition_id}");
        spawn_blocking(move || {
            if let Ok(res) = conn.query_row::<String, _, _>(sql.as_str(), (), |row| row.get(0)) {
                let blockpart: BlockPartition = serde_json::from_str(&res)
                    .map_err(|e| IndexError::InvalidDatabaseContent(e.into()))?;
                Ok(Some(blockpart))
            } else {
                Ok(None)
            }
        })
        .await
        .expect("blocking task shouldnt panic")
    }

    /// Insert a new table into the chain data index. This will error out
    /// if a table with the same name already exists.
    pub async fn new_table(&self, name: &str, blocknum_col: &str) -> Result<TablePartitionIndex> {
        TablePartitionIndex::init(
            self.conn.clone(),
            name,
            blocknum_col,
            self.blocks_per_partition,
            None,
        )
        .await
    }
    /// Take a [`TableApi`] trait object and add it into the index.
    pub async fn register_table(&self, table: Arc<dyn TableApi>) -> Result<TablePartitionIndex> {
        let name = table.name();
        let col = table.blocknum_col();
        let schema = Arc::new(table.schema());
        TablePartitionIndex::init(
            self.conn.clone(),
            name,
            col,
            self.blocks_per_partition,
            Some(schema),
        )
        .await
    }

    /// Convenience method to add a partition to a table in this index. Since
    /// [`BlockPartition`] has field `table` it can be used to look up the table to write to.
    pub async fn add_partition(&self, partition: BlockPartition) -> Result<()> {
        if partition.lower_bound % self.blocks_per_partition != 0 {
            bail!("partition lower bound is not a multiple of blocks per partition");
        }
        let mut conn = self.acquire_conn().await?;
        spawn_blocking(move || {
            let tx = conn.transaction()?;
            TablePartitionIndex::write_partition_helper(tx, partition)?;
            Ok(())
        })
        .await
        .expect("blocking task shouldnt panic")
        .map_err(IndexError::SqliteError)?;
        Ok(())
    }

    /// Load from the given persistence layer `store`.
    pub async fn load(store: &StorageApi<Self>) -> Result<Self> {
        let dat = store
            .load()
            .await
            .map_err(|e| anyhow!(e).context("failed to load from store"))?;
        dat.ok_or_else(|| anyhow!("no loading error occurred but {store:?} was empty"))
    }
    /// Persist the current state using `store`.
    pub async fn save(&self, store: &StorageApi<Self>) -> Result<()> {
        store.save(self).await
    }

    pub fn chain_id(&self) -> &str {
        &self.id
    }
    /// Return a list of all table names in this index.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let conn = self.acquire_conn().await?;
        spawn_blocking(move || {
            let mut stmt = conn.prepare("SELECT name FROM all_tables")?;
            let mut rows = stmt.query([])?;
            let mut names: Vec<String> = Vec::new();
            while let Some(row) = rows.next()? {
                names.push(row.get(0)?);
            }
            Ok(names) as Result<_, rusqlite::Error>
        })
        .await
        .expect("blocking task shouldnt panic")
        .map_err(|e| IndexError::SqliteError(e).into())
    }

    /// Return all tables in this index.
    pub async fn all_tables(&self) -> Result<Vec<TablePartitionIndex>> {
        let conn = self.acquire_conn().await?;
        let triples = spawn_blocking(move || {
            // name TEXT NOT NULL,
            //     blocknum_column TEXT NOT NULL,
            //     blocks_per_partition INTEGER NOT NULL,
            let mut stmt = conn.prepare(
                "
                SELECT name, blocknum_column, blocks_per_partition, arrow_schema_blob, total_row_count, total_byte_count
                FROM all_tables",
            )?;
            let mut rows = stmt.query([])?;
            let mut names: Vec<TableRow> = Vec::new();
            while let Some(row) = rows.next()? {
                names.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?));
            }
            Result::<_, rusqlite::Error>::Ok(names)
        })
        .await
        .expect("blocking task shouldnt panic")
        .map_err(IndexError::SqliteError)?;
        Ok(triples
            .into_iter()
            .map(
                |(name, col, blocks_per_partition, schema_blob, rowc, bytec)| {
                    let schema = schema_blob
                        .map(|blob| bytes_to_schema(blob.into()))
                        .map_or(Ok(None), |inner| inner.map(Some))?;
                    Ok(TablePartitionIndex {
                        name,
                        blocks_per_partition,
                        blocknum_column: col,
                        conn: self.conn.clone(),
                        schema,
                        byte_count: Arc::new(parking_lot::Mutex::new(bytec)),
                        row_count: Arc::new(parking_lot::Mutex::new(rowc)),
                    }) as Result<TablePartitionIndex, IndexError>
                },
            )
            .try_collect()?)
    }

    /// Get a [`PartitionedTable`] given its name. This will return `None` if the
    /// table does not exist. Will panic on unexpected SQLite errors.
    pub async fn get_table(&self, table: &str) -> Result<Option<TablePartitionIndex>> {
        let conn = self.acquire_conn().await?;
        let name = table.to_owned();
        let res: Option<TableRow> = spawn_blocking(move || {
            // name TEXT NOT NULL,
            //     blocknum_column TEXT NOT NULL,
            //     blocks_per_partition INTEGER NOT NULL,
            if let Ok(res) = conn.query_row(
                "
                SELECT 
                    name, blocknum_column, blocks_per_partition, 
                    arrow_schema_blob, total_row_count, total_byte_count
                FROM all_tables
                WHERE name=?1",
                params![name],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            ) {
                Some(res)
            } else {
                None
            }
        })
        .await
        .expect("blocking task shouldnt panic");
        if let Some((name, col, blocks_per_partition, schema_blob, rowc, bytec)) = res {
            let schema = schema_blob
                .map(|blob| bytes_to_schema(blob.into()))
                .map_or(Ok(None), |inner| inner.map(Some))?;
            Ok(Some(TablePartitionIndex {
                name,
                blocks_per_partition,
                blocknum_column: col,
                conn: self.conn.clone(),
                schema,
                byte_count: Arc::new(parking_lot::Mutex::new(bytec)),
                row_count: Arc::new(parking_lot::Mutex::new(rowc)),
            }))
        } else {
            Ok(None)
        }
    }

    /// tables controlled by this is just a lookup table that contains the names
    /// of all the tables and then a table for the metadata.
    const fn ddl() -> (&'static str, &'static str) {
        (
            "CREATE TABLE IF NOT EXISTS all_tables (
                name TEXT NOT NULL,
                blocknum_column TEXT NOT NULL,
                blocks_per_partition INTEGER NOT NULL,
                arrow_schema_blob BLOB,
                total_row_count INTEGER NOT NULL,
                total_byte_count INTEGER NOT NULL,
                PRIMARY KEY (name)
            );",
            // this table should always be one row
            "CREATE TABLE IF NOT EXISTS chain_data (
                chain TEXT NOT NULL, 
                blocks_per_partition INTEGER NOT NULL,
                -- json metadata
                metadata TEXT,
                PRIMARY KEY (chain)
            )",
        )
    }

    pub async fn get_metadata(&self) -> Result<HashMap<String, String>> {
        let conn = self.acquire_conn().await?;
        spawn_blocking(move || {
            let metadata: Option<String> =
                conn.query_row("SELECT metadata FROM chain_data", (), |row| row.get(0))?;
            if let Some(json) = metadata {
                let map: HashMap<String, String> = serde_json::from_str(&json)?;
                Ok(map)
            } else {
                Result::<_, anyhow::Error>::Ok(HashMap::new())
            }
        })
        .await
        .expect("blocking task shouldnt panic")
    }

    pub async fn add_metadata(&self, key: &str, val: &str) -> Result<()> {
        let mut map = self.get_metadata().await?;
        map.insert(key.to_owned(), val.to_owned());
        let conn = self.acquire_conn().await?;
        spawn_blocking(move || {
            let s = serde_json::to_string(&map)?;
            conn.execute("UPDATE chain_data SET metadata=?1;", params![s])?;
            Ok(())
        })
        .await
        .expect("blocking task shouldnt panic")
    }
    /// total amount of partitions across all tables in this index
    pub async fn partition_count(&self) -> Result<usize> {
        let tables = self.table_names().await?;
        let conn = self.acquire_conn().await?;
        // run it all in one connection instead of using the table methods
        spawn_blocking(move || {
            let mut total = 0;
            for t in tables {
                let subsum: usize = conn.query_row(
                    format!(
                        "select count(*) 
                         from {t}_partitions "
                    )
                    .as_str(),
                    (),
                    |row| row.get(0),
                )?;
                total += subsum;
            }
            Ok(total) as Result<usize, rusqlite::Error>
        })
        .await
        .expect("blocking task shouldnt panic")
        .map_err(|e| IndexError::SqliteError(e).into())
    }
}

/// data structure for managing shared sqlite db connection. when indices are instantiated
/// they use a [`NamedTempFile`] for the sqlite db. This db can be moved to a permanent
/// file path, for example when we know a location where a user would like us to put
/// app data.
#[derive(Debug, Clone)]
struct Conn {
    /// Shared mutable reference to the temporary file if this index is backed by a temp file.
    /// This reference is shared by other tables that came from the same chain.
    /// If this index was created outside of the context of a chain, then there will
    /// be no tables with this shared ref.
    tempfile: Arc<RwLock<Option<NamedTempFile>>>,
    /// Permanent path on disk
    db_path: Arc<RwLock<Option<PathBuf>>>,
    /// mutex for sqlite connections
    conn_mutex: Arc<Mutex<()>>,
}
impl Conn {
    fn new() -> Self {
        Self {
            tempfile: Arc::new(RwLock::new(Some(
                NamedTempFile::new().expect("should be able to create a tempfile"),
            ))),
            db_path: Arc::new(RwLock::new(None)),
            conn_mutex: Arc::new(Mutex::new(())),
        }
    }

    async fn get_db_path(&self) -> PathBuf {
        let dbpath = self.db_path.clone().read_owned().await;
        let tempfile = self.tempfile.clone().read_owned().await;
        if dbpath.is_some() {
            dbpath.as_ref().unwrap().to_owned()
        } else if tempfile.is_some() {
            tempfile.as_ref().unwrap().path().to_owned()
        } else {
            panic!("one of tempfile or permanent db path must exist")
        }
    }

    /// Connection should always be able to be acquired (after mutex is released).
    async fn acquire_conn(&self) -> Result<ConnGuard> {
        let dur = Duration::from_millis(1000);
        let mutex = tokio::time::timeout(dur, self.conn_mutex.clone().lock_owned())
            .await
            .map_err(IndexError::DeadlockTimeout)?;
        let db_path = self.get_db_path().await;
        let conn = spawn_blocking(move || {
            // move the mutex into Conn so that it gets dropped when connection does
            let conn = Connection::open(db_path)
                .expect("sqlite database should already exist and be open-able");
            if !conn.is_autocommit() {
                panic!(
                    "opened sqlite connection is not autocommit! \
                     code assumes connections are always autocommit!"
                );
            }
            ConnGuard { mutex, conn }
        })
        .await
        .expect("blocking task shouldnt panic");
        Ok(conn)
    }

    /// Deletes the underlying sqlite db on disk.
    /// This is useful for testing, but should not be used in production.
    async fn destroy_storage(&self) {
        let dbpath = self.db_path.clone().read_owned().await;
        let mut tempfile = self.tempfile.clone().write_owned().await;
        let old_path = if dbpath.is_some() {
            dbpath.as_ref().unwrap().to_owned()
        } else if tempfile.is_some() {
            let oldpath = tempfile.as_ref().unwrap().path().to_owned();
            // remove tempfile
            *tempfile = None;
            oldpath
        } else {
            panic!("one of tempfile or permanent db path must exist")
        };
        let removed = tokio::fs::remove_file(&old_path).await;
        if removed.is_err() {
            warn!("failed to remove sqlite db at path: {:?}", old_path);
        }
    }
    /// Switch the underlying db to be stored on the given path.
    /// This is usually useful when we have a location on disk that we can store
    /// the sqlite db (instead of using the tempfile)
    pub async fn db_set_local_disk_path(&mut self, path: impl AsRef<Path>) -> Result<()> {
        // want no one else is using the conn, but we dont
        // need to actually use the conn, so we only acquire the mutex
        let _guard = self.conn_mutex.lock().await;
        // acquire locks on shared refs to update them
        let mut dbpath = self.db_path.clone().write_owned().await;
        let mut tempfile = self.tempfile.clone().write_owned().await;
        let old_path = if dbpath.is_some() {
            let prevpath = dbpath.as_ref().unwrap();
            tokio::fs::rename(prevpath, path.as_ref())
                .await
                .map_err(IndexError::DiskIoError)?;
            prevpath.to_owned()
        } else if tempfile.is_some() {
            let prevpath = tempfile.as_ref().unwrap().path().to_owned();
            tokio::fs::rename(&prevpath, path.as_ref())
                .await
                .map_err(IndexError::DiskIoError)?;
            // remove tempfile
            *tempfile = None;
            prevpath
        } else {
            panic!("one of tempfile or permanent db path must exist")
        };
        // update self.db_path
        *dbpath = Some(path.as_ref().to_owned());
        if let Err(e) = tokio::fs::remove_file(&old_path).await {
            warn!(
                "failed to cleanup old sqlite db at path {} due to {e:?}",
                old_path.display()
            );
        }
        Ok(())
    }
}

/// simple struct for wrapping a connection and a mutex guard so that they
/// get moved and dropped together
#[derive(Debug)]
struct ConnGuard {
    conn: Connection,
    mutex: OwnedMutexGuard<()>,
}
impl std::ops::Deref for ConnGuard {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}
impl std::ops::DerefMut for ConnGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

/// lil macro just to implement some shared util functions on the underlying sqlite connection.
/// takes in an ident for the struct name, and the struct must have fields:
/// - db_path
/// - tempfile
/// - conn_mutex
macro_rules! impl_db_util {
    ($name: ident) => {
        impl $name {
            async fn get_db_path(&self) -> PathBuf {
                self.conn.get_db_path().await
            }
            pub async fn db_set_local_disk_path(&mut self, path: impl AsRef<Path>) -> Result<()> {
                self.conn.db_set_local_disk_path(path).await
            }
            /// Connection should always be able to be acquired (after mutex is released).
            async fn acquire_conn(&self) -> Result<ConnGuard> {
                self.conn.acquire_conn().await
            }
            /// Deletes the underlying sqlite db on disk.
            /// This is useful for testing, but should not be used in production.
            async fn destroy_storage(&self) {
                self.conn.destroy_storage().await
            }
        }
    };
}

fn schema_to_bytes(schema: Arc<Schema>) -> Result<Bytes> {
    let empty = RecordBatch::new_empty(schema.clone());
    let buf = SwappableMemBuf::new();
    let mut writer = FileWriter::try_new(buf.clone(), schema.as_ref())?;
    writer.write(&empty)?;
    writer.finish()?;
    Ok(buf.flush_empty())
}

fn bytes_to_schema(bytes: Bytes) -> Result<Arc<Schema>> {
    let buf = io::Cursor::new(bytes);
    let rdr = FileReader::try_new(buf, None)?;
    Ok(rdr.schema())
}

// // give both these db util methods. these methods just rely on the existence/types
// // of tempfile and conn_ref
impl_db_util!(TablePartitionIndex);
impl_db_util!(ChainPartitionIndex);

#[cfg(test)]
mod tests {
    use crate::{
        chains::{
            test::{empty_chain_table, TestChain},
            ChainDef,
        },
        storage::StorageConf,
        table_api::BlockNumSet,
        test::{randbytes, TestDir},
    };
    use futures::stream::StreamExt;

    use super::*;
    /// return a random path for a db for testing
    fn dbpath() -> PathBuf {
        PathBuf::new()
            .join(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join(format!("_test{}.db", hex::encode(randbytes(8))))
    }

    macro_rules! partition {
        ($bound: expr) => {
            BlockPartition {
                table: "test".to_owned(),
                lower_bound: $bound,
                location: Location::new("file", None, "/"),
                created_at: DateTime::parse_from_str(
                    "1983 Apr 13 12:09:14.274 +0000",
                    "%Y %b %d %H:%M:%S%.3f %z",
                )
                .unwrap()
                .into(),
                byte_count: Some(1),
                row_count: Some(1),
                ..Default::default()
            }
        };
    }

    #[tokio::test]
    async fn test_schema_to_byte() {
        let table = empty_chain_table();
        let batch = table
            .batch_for_blocknums(&BlockNumSet::Range(0, 10))
            .await
            .unwrap();
        let schema = batch.schema();
        let bytes = schema_to_bytes(schema.clone()).unwrap();
        let newschema = bytes_to_schema(bytes).unwrap();
        assert_eq!(schema, newschema);
    }

    #[tokio::test]
    async fn test_new_table_db_utils() -> Result<()> {
        let mut t = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        assert!(t.is_valid().await?);
        assert_eq!(t.name(), "test");
        assert_eq!(t.blocknum_column(), "blocknum");
        let conn = t.acquire_conn().await?;
        let path = conn.path().unwrap().to_owned();
        drop(conn);
        let newpath = dbpath();
        t.conn.db_set_local_disk_path(&newpath).await.unwrap();
        let newdbpath = t.acquire_conn().await?.path().unwrap().to_owned();
        assert_eq!(newpath, newdbpath);
        tokio::fs::read(&path)
            .await
            .expect_err("expected tempfile to longer exist");
        t.destroy_storage().await;
        Ok(())
    }
    macro_rules! conn_paths_equal {
        ($elem1: ident, $elem2: ident) => {
            let p1 = async {
                let conn = $elem1.acquire_conn().await.unwrap();
                conn.path().unwrap().to_owned()
            }
            .await;
            let p2 = async {
                let conn = $elem2.acquire_conn().await.unwrap();
                conn.path().unwrap().to_owned()
            }
            .await;
            assert_eq!(p1, p2);
        };
    }

    async fn tempfile_path(t: &TablePartitionIndex) -> Option<PathBuf> {
        let tf = t.conn.tempfile.clone().read_owned().await;
        tf.as_ref().map(|tf| tf.path().to_owned())
    }

    #[tokio::test]
    async fn test_table_shared_resources_update() -> Result<()> {
        let mut t = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        let t2 = TablePartitionIndex::init(t.conn.clone(), "test2", "blocknum", 100, None)
            .await
            .unwrap();
        conn_paths_equal!(t, t2);
        assert_eq!(tempfile_path(&t).await, tempfile_path(&t2).await);
        t.conn.db_set_local_disk_path(&dbpath()).await.unwrap();
        conn_paths_equal!(t, t2);
        assert!(tempfile_path(&t).await.is_none());
        t.destroy_storage().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_table_invalid() {
        let t = TablePartitionIndex::try_new("test", "blocknum", 100)
            .await
            .unwrap();
        async {
            let conn = t.acquire_conn().await.unwrap();
            conn.execute("drop table test_partitions", ()).unwrap();
        }
        .await;
        assert!(!t.is_valid().await.unwrap());
        t.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_table_add_partition() -> Result<()> {
        let t = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        // add one partition id 0
        t.add_partition(partition!(0)).await?;
        let stats = t.stats();
        assert_eq!(stats.num_rows.unwrap(), 1);
        assert_eq!(stats.total_byte_size.unwrap(), 1);
        assert_eq!(t.get_partition(0).await?, Some(partition!(0)));
        assert_eq!(t.partition_ids().await?, vec![0]);
        // add partition with same id
        t.add_partition(partition!(0)).await.unwrap();
        let stats = t.stats();
        assert_eq!(stats.num_rows.unwrap(), 1);
        assert_eq!(stats.total_byte_size.unwrap(), 1);
        assert_eq!(t.partition_ids().await?, vec![0]);
        // add a different id partition
        t.add_partition(partition!(100)).await.unwrap();
        assert_eq!(t.partition_ids().await?, vec![0, 100]);
        assert_eq!(t.get_partition(100).await?, Some(partition!(100)));
        let stats = t.stats();
        assert_eq!(stats.num_rows.unwrap(), 2);
        assert_eq!(stats.total_byte_size.unwrap(), 2);
        // get id that doesnt exist
        assert_eq!(t.get_partition(1000).await?, None);
        // add bad lower bound
        t.add_partition(partition!(99)).await.unwrap_err();
        // add invalid table name
        t.add_partition(BlockPartition {
            table: "bad".to_owned(),
            lower_bound: 0,
            location: Location::new("file", None, "/"),
            ..Default::default()
        })
        .await
        .unwrap_err();
        // stats should be unchanged
        let stats = t.stats();
        assert_eq!(stats.num_rows.unwrap(), 2);
        assert_eq!(stats.total_byte_size.unwrap(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_table_merge_partitions_valid() -> Result<()> {
        let t1 = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        let t2 = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        // add one partition id 0
        t1.add_partition(partition!(0)).await?;
        t2.add_partition(partition!(100)).await?;
        t2.add_partition(partition!(200)).await?;
        t1.merge_block_partitions(&t2).await?;
        assert_eq!(t1.partition_ids().await?.len(), 3);
        t1.get_partition(100).await.unwrap();
        assert_eq!(t1.stats().num_rows.unwrap(), 3);
        Ok(())
    }
    #[tokio::test]
    async fn test_table_merge_partitions_overlap_partitions() -> Result<()> {
        let t1 = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        let t2 = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        // add one partition id 0
        t1.add_partition(partition!(0)).await?;
        t2.add_partition(partition!(0)).await?;
        t2.add_partition(partition!(200)).await?;
        t1.merge_block_partitions(&t2).await.unwrap_err();
        Ok(())
    }
    #[tokio::test]
    async fn test_table_merge_partitions_invalid_props() -> Result<()> {
        let t1 = TablePartitionIndex::try_new("test", "blocknum", 100).await?;
        let t2 = TablePartitionIndex::try_new("test2", "blocknum", 100).await?;
        t1.merge_block_partitions(&t2).await.unwrap_err();
        let t2 = TablePartitionIndex::try_new("test", "blocknum", 1000).await?;
        t1.merge_block_partitions(&t2).await.unwrap_err();
        Ok(())
    }

    #[tokio::test]
    async fn test_table_partition_last_partition() {
        let t = TablePartitionIndex::try_new("test", "blocknum", 100)
            .await
            .unwrap();
        t.add_partition(partition!(0)).await.unwrap();
        t.add_partition(partition!(100)).await.unwrap();
        let last = t.last_partition().await.unwrap().unwrap();
        assert_eq!(last, partition!(100));
        t.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_chain_add_partition() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        t0.new_table("test", "testy").await.unwrap();
        t0.add_partition(partition!(0)).await.unwrap();
        t0.add_partition(partition!(0)).await.unwrap();
        // add partition with bad boundary
        t0.add_partition(partition!(99)).await.unwrap_err();
        // add partition with bad name
        t0.add_partition(BlockPartition {
            table: "bad".to_owned(),
            lower_bound: 0,
            location: Location::new("file", None, "/"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_chain_all_tables() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        for i in 0..10 {
            t0.new_table(&format!("test{i}"), "testy").await.unwrap();
        }
        let alltables = t0.all_tables().await.unwrap();
        assert_eq!(alltables.len(), 10);
    }

    #[tokio::test]
    async fn test_chain_get_set_metadata() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        assert!(t0.get_metadata().await.unwrap().is_empty());
        t0.add_metadata("test", "testy").await.unwrap();
        assert_eq!(
            t0.get_metadata().await.unwrap().get("test").unwrap(),
            "testy"
        );
    }

    #[tokio::test]
    async fn test_chain_index_persistable() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let t1 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let bytes = t0.to_bytes().await.unwrap();
        let t2 = ChainPartitionIndex::from_bytes(&bytes).await.unwrap();
        assert_eq!(t1.id, t2.id);
        t0.destroy_storage().await;
        t1.destroy_storage().await;
        t2.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_chain_data_write_to_store() {
        let dir = TestDir::new(true);
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: dir.path.clone(),
            filename: "testy.json".to_string(),
        })
        .await
        .unwrap();
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        t0.new_table("test", "col").await.unwrap();
        t0.add_partition(partition!(0)).await.unwrap();
        t0.add_partition(partition!(100)).await.unwrap();
        t0.add_partition(partition!(200)).await.unwrap();
        t0.save(&store).await.unwrap();
        let newchain = ChainPartitionIndex::load(&store).await.unwrap();
        assert_eq!(newchain.chain_id(), t0.chain_id());
        t0.destroy_storage().await;
    }
    #[tokio::test]
    async fn test_table_stream_partitions() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        t0.new_table("test", "col").await.unwrap();
        for i in (0..10_000).step_by(100) {
            t0.add_partition(partition!(i)).await.unwrap();
        }
        let table = t0.get_table("test").await.unwrap().unwrap();
        let mut stream = table.stream_block_partitions(10);
        let mut idx = 0;
        while let Some(item) = stream.next().await {
            let partitions = item.unwrap();
            assert_eq!(partitions.len(), 10);
            let lowerbound = idx * 1000;
            assert_eq!(partitions[0].lower_bound, lowerbound);
            assert_eq!(partitions[9].lower_bound, lowerbound + 900);
            idx += 1;
        }
        t0.destroy_storage().await;
    }
    #[tokio::test]
    async fn test_table_stream_partitions_one_batch() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        t0.new_table("test", "col").await.unwrap();
        for i in (0..10_000).step_by(100) {
            t0.add_partition(partition!(i)).await.unwrap();
        }
        let table = t0.get_table("test").await.unwrap().unwrap();
        let mut stream = table.stream_block_partitions(999999);
        stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn test_table_stream_partitions_empty() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        t0.new_table("test", "col").await.unwrap();
        let table = t0.get_table("test").await.unwrap().unwrap();
        let mut stream = table.stream_block_partitions(999999);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_sqlite_set_disk_path_from_tempfile() {
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let ogbytes = t0.to_bytes().await.unwrap();
        let mut t2 = ChainPartitionIndex::from_bytes(&ogbytes).await.unwrap();
        assert!(t2.conn.tempfile.clone().read_owned().await.is_some());
        let p = dbpath();
        t2.conn.db_set_local_disk_path(p.clone()).await.unwrap();
        let diskbytes = std::fs::read(p.clone()).unwrap();
        assert_eq!(diskbytes, ogbytes);
        t0.destroy_storage().await;
        t2.destroy_storage().await;
        cleanfiles(vec![p]);
    }
    #[tokio::test]
    async fn test_sqlite_set_disk_path_from_non_tempfile() {
        let mut t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let p1 = dbpath();
        let p2 = dbpath();
        t0.conn.db_set_local_disk_path(&p1).await.unwrap();
        t0.conn.db_set_local_disk_path(&p2).await.unwrap();
        std::fs::read(&p1).unwrap_err();
        std::fs::read(&p2).unwrap();
        cleanfiles(vec![p1, p2]);
    }

    #[allow(unused_must_use)]
    fn cleanfiles(fs: Vec<PathBuf>) {
        for f in fs {
            std::fs::remove_file(f);
        }
    }

    #[tokio::test]
    async fn test_chain_index_new_table() {
        let idx = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let table = idx.new_table("table1", "col").await.unwrap();
        assert_eq!(table.name(), "table1");
        let table2 = idx.new_table("table2", "col").await.unwrap();
        assert_eq!(table2.name(), "table2");
        idx.new_table("table2", "col").await.unwrap_err();
        idx.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_chain_index_get_table() {
        let idx = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let table = idx.new_table("table1", "col").await.unwrap();
        let got_table = idx.get_table("table1").await.unwrap().unwrap();
        assert_eq!(table.name(), got_table.name());
        assert_eq!(table.blocknum_column(), got_table.blocknum_column());
        assert_eq!(
            table.blocks_per_partition(),
            got_table.blocks_per_partition()
        );
        idx.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_chain_index_add_partition() {
        let idx = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let table = idx.new_table("table1", "col").await.unwrap();
        assert_eq!(table.name(), "table1");
        let table2 = idx.new_table("table2", "col").await.unwrap();
        assert_eq!(table2.name(), "table2");
        idx.new_table("table2", "col").await.unwrap_err();
        idx.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_multi_writers_and_save() {
        let dir = TestDir::new(true);
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: dir.path.clone(),
            filename: "testy.json".to_string(),
        })
        .await
        .unwrap();
        let idx = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let mut futs = vec![];
        let mut tables = vec![];
        for n in 0..5 {
            let tname = format!("t{n}");
            let index = idx.clone();
            let table = index.new_table(&tname, "col").await.unwrap();
            tables.push(table.clone());
            futs.push(tokio::task::spawn(async move {
                index
                    .add_partition(BlockPartition {
                        table: tname,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }));
        }
        futures::future::join_all(futs.into_iter()).await;
        let part_count = idx.partition_count().await.unwrap();
        assert_eq!(part_count, 5);
        idx.save(&store).await.unwrap();
        let newchain = ChainPartitionIndex::load(&store).await.unwrap();
        assert_eq!(newchain.partition_count().await.unwrap(), 5);
        let table1 = &tables[0];
        for t in &tables {
            conn_paths_equal!(table1, t);
        }
        conn_paths_equal!(table1, idx);
        idx.destroy_storage().await;
    }

    #[tokio::test]
    async fn test_create_table_with_schema() {
        let table = empty_chain_table();
        let t0 = ChainPartitionIndex::try_new("testy", 100).await.unwrap();
        let table = t0.register_table(table).await.unwrap();
        // load same table from sqlite db
        let same_table = t0.get_table(&table.name).await.unwrap().unwrap();
        let init_schema = table.schema.unwrap();
        let same_schema = same_table.schema.unwrap();
        assert!(!Arc::ptr_eq(&init_schema, &same_schema));
        assert_eq!(init_schema, same_schema);
    }

    #[tokio::test]
    async fn test_create_partition() {
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::Memory {
            bucket: "testy".to_owned(),
        })
        .await
        .unwrap();
        let table = empty_chain_table();
        let partsize = TestChain::BLOCKS_PER_PARTITION;
        let t0 = ChainPartitionIndex::try_new("testy", partsize)
            .await
            .unwrap();
        t0.register_table(table.clone()).await.unwrap();
        let part = t0
            .create_partition(
                0,
                table,
                &store,
                CreatePartitionOpts {
                    batches_per_row_group: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(part.lower_bound, 0);
        assert!(!part.incomplete);
        assert_eq!(part.row_count.unwrap(), partsize);
    }
    #[tokio::test]
    async fn test_create_partition_with_end() {
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::Memory {
            bucket: "testy".to_owned(),
        })
        .await
        .unwrap();
        let table = empty_chain_table();
        let partsize = TestChain::BLOCKS_PER_PARTITION;
        let t0 = ChainPartitionIndex::try_new("testy", partsize)
            .await
            .unwrap();
        t0.register_table(table.clone()).await.unwrap();
        let part = t0
            .create_partition(
                0,
                table,
                &store,
                CreatePartitionOpts {
                    end: Some(25),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(part.lower_bound, 0);
        assert!(part.incomplete);
        assert_eq!(part.row_count.unwrap(), 25);
    }
}
