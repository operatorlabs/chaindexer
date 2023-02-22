//! Define an interface for interacting with tables. This interfaces handles both
//! data fetching (e.g. creating a table of all ethereum blocks using an rpc api), and
//! querying.
use crate::partition_index::TablePartitionIndex;
use crate::util::SwappableMemBuf;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_plan::RecordBatchStream;
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result as DataFusionResult},
};
use futures::{stream, Stream, StreamExt};
use itertools::Itertools;
use log::{debug, error, info, warn};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::task::Poll;
use std::{pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Error)]
pub enum TableApiError {
    /// Error related to Arrow data occurred. this is often due to data not fitting the
    /// expected schemas (for example a null value in a non-null [datafusion::arrow::datatypes::Field])
    #[error("datafusion error occurred")]
    DataFusion(#[from] DataFusionError),
    /// Error occurred within data fetching logic. For example, an ethereum RPC call failed.
    #[error("error during data fetching")]
    DataFetching(#[from] anyhow::Error),
}
impl From<TableApiError> for DataFusionError {
    fn from(value: TableApiError) -> Self {
        match value {
            TableApiError::DataFusion(e) => e,
            TableApiError::DataFetching(e) => {
                DataFusionError::External(e.context("from TableApiError::DataFetching").into())
            }
        }
    }

    // fn into(self) -> DataFusionError {
    //     match self {
    //         TableApiError::DataFusion(e) => e,
    //         TableApiError::DataFetching(e) => {
    //             DataFusionError::External(e.context("from TableApiError::DataFetching").into())
    //         }
    //     }
    // }
}
pub(crate) type TableRef = Arc<dyn TableApi>;
/// Trait defines the interface.
#[async_trait]
pub trait TableApi: Send + Sync + std::fmt::Debug + 'static {
    /// name of the table
    fn name(&self) -> &str;
    /// get the schema of the arrow record batches returned
    fn schema(&self) -> Schema;
    /// get record batch for blocks in the range start-end.
    async fn batch_for_blocknums(
        &self,
        blocknums: &BlockNumSet,
    ) -> Result<RecordBatch, TableApiError>;
    /// get (live) max block number
    async fn max_blocknum(&self) -> Result<Option<u64>, TableApiError>;
    /// name of column that represents the block number.
    ///
    /// ### this will panic if column is not real
    fn blocknum_col(&self) -> &str;
    /// Get the underlying data for this table if it exists. Used within query engine.
    async fn partition_data(&self) -> Result<Option<TablePartitionIndex>, TableApiError>;
    /// lists all column names
    fn col_names(&self) -> Vec<String>;
    /// get a [`TableApiStream`] for blocks in `blocknums`
    ///
    /// - `blocks_per_batch` specifies how many blocks worth of data
    /// should each [`RecordBatch`] contain.
    ///
    /// - `count_chan` an optional channel that sends the number of rows processed
    ///   and the next block it will start at after the creation of each [`RecordBatch`]
    fn stream_batches(
        self: Arc<Self>,
        blocknums: &BlockNumSet,
        blocks_per_batch: u64,
        count_chan: Option<mpsc::UnboundedSender<(u64, u64)>>,
    ) -> TableApiStream;
}

enum StreamState {
    /// fetch data starting from this index
    Fetching(usize),
    /// exit the stream (either done or returning early due to error)
    Done,
}
type BatchStreamType = Pin<Box<dyn Stream<Item = DataFusionResult<RecordBatch>> + Send>>;

/// given a trait object implementing [`TableApi`] stream record batches
/// via [`TableApi::batch_for_blocknums`].
///
/// each yielded record batch (other than maybe the final one)
/// will have `blocks_per_batch` worth of data in it.
///
/// implements datafusion [`RecordBatchStream`] meaning it can be
/// returned from physical plans. also can be converted into a stream that yields
/// raw bytes as a parquet file for uploading to storage layer!
pub struct TableApiStream {
    table: TableRef,
    stream: BatchStreamType,
    schema: SchemaRef,
}

impl TableApiStream {
    fn init_stream(
        table: TableRef,
        blocknums: &BlockNumSet,
        blocks_per_batch: u64,
        count_chan: Option<mpsc::UnboundedSender<(u64, u64)>>,
    ) -> BatchStreamType {
        let chunks = blocknums.chunks(usize::max(blocknums.len() / blocks_per_batch as usize, 1));
        if chunks.is_empty() {
            return Box::pin(futures::stream::empty());
        }
        let chunks = Arc::new(chunks.into_iter().map(|c| c.owned()).collect_vec());
        let table_ = table.clone();
        // let indices = (0..blocknums.len()).collect_vec();
        let end_idx = chunks.len() - 1;
        let cur_idx = 0;
        let stream = Box::pin(stream::unfold(
            StreamState::Fetching(cur_idx),
            move |state| {
                let table = table_.clone();
                let chan = count_chan.clone();
                let chunks = chunks.clone();
                async move {
                    match state {
                        StreamState::Fetching(idx) => {
                            let chunk = chunks.get(idx).unwrap().to_owned();
                            if chunk.as_set().is_empty() {
                                return None;
                            }

                            let next_state = if idx == end_idx {
                                StreamState::Done
                            } else {
                                StreamState::Fetching(idx + 1)
                            };

                            let res = table.batch_for_blocknums(&chunk.as_set()).await;

                            match res {
                                Ok(batch) => {
                                    if let Some(tx) = chan {
                                        let lower = chunk.as_set().iter().next().unwrap();
                                        tx.send((batch.num_rows() as u64, lower)).ok();
                                    }
                                    Some((Ok(batch), next_state))
                                }
                                Err(err) => Some((Err(err.into()), StreamState::Done)),
                            }
                        }
                        StreamState::Done => {
                            debug!("done streaming batches for table {}", table.name());
                            None
                        }
                    }
                }
            },
        ));
        Box::pin(stream)
    }
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.stream = Box::pin(TableApiStreamLimit {
            schema: self.schema.clone(),
            inner: self.stream,
            limit,
            total: 0,
        });
        self
    }
    /// Initialize a new stream. Pass in `count_chan` to get updates of (row_count, lower_bound)
    /// after each yield of the stream.
    pub fn new(
        table: Arc<dyn TableApi>,
        blocknums: &BlockNumSet,
        blocks_per_batch: u64,
        // yield tuples of (row_count, lower_bound) after each iteration
        count_chan: Option<mpsc::UnboundedSender<(u64, u64)>>,
    ) -> Self {
        let schema = Arc::new(table.schema());
        let stream = Self::init_stream(table.clone(), blocknums, blocks_per_batch, count_chan);
        Self {
            table,
            stream,
            schema,
        }
    }

    /// Convert into a byte stream in parquet format.
    pub fn into_parquet_bytes(
        mut self,
        batches_per_rowgroup: Option<NonZeroUsize>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send>> {
        let rowgroup_size = batches_per_rowgroup
            .unwrap_or(NonZeroUsize::new(1).unwrap())
            .get();
        let schema = Arc::new(self.table.schema());
        let tablename = self.table.name().to_owned();
        let s = async_stream::stream! {
            let buff = SwappableMemBuf::new();
            let writer = Arc::new(Mutex::new(ArrowWriter::try_new(
                buff.clone(),
                schema.clone(),
                Some(
                    WriterProperties::builder()
                        // row group sizes are managed by flushing after each chunk
                        .set_max_row_group_size(usize::MAX)
                        .set_statistics_enabled(EnabledStatistics::Page)
                        .build(),
                ),
            ).expect("failed to create writer")));

            let mut itercounter = 0; // counts iterations
            let mut success = true; // flag set false on errors
            // iterate over the entire record batch stream
            while let Some(v) = self.stream.next().await {
                itercounter += 1;
                match v {
                    Ok(batch) => {
                        let numrows = batch.num_rows();
                        info!("{tablename} got new batch containing {numrows} rows");
                        debug!("{} writing rowgroup of {numrows}", tablename);
                        let buff = buff.clone();
                        let writer = writer.clone();
                        if itercounter % rowgroup_size == 0 {
                            // flush rowgroup,  get all the bytes from the buff, then yield
                            let mut writer = writer.lock().await;
                            writer.write(&batch).expect("failed to write RecordBatch \
                                                         in parquet format");
                            writer.flush().expect("parquet writer failed to flush to rowgroup");
                            let bytes = buff.flush_empty();
                            yield Ok(bytes);
                        } else {
                            // write data but dont flush and yield
                            let mut writer = writer.lock().await;
                            writer.write(&batch).expect("failed to write RecordBatch \
                                                         in parquet format");
                        }
                    },
                    Err(err) => {
                        success = false;
                        yield Err(
                            anyhow!(err)
                                .context("Got error while fetching \
                                          record batches thru table api")
                        );
                        break;
                    }
                };
            }
            if success {
                // write final parquet shit (and rows that got passed over)
                // =======================

                // sorta hacky way to get ownership over writer.
                let mut writer = writer.lock().await;
                let dummy = ArrowWriter::try_new(SwappableMemBuf::new(), schema, None)
                    .expect("could not intialize writer");
                let owned = std::mem::replace(&mut *writer, dummy);
                // close writer will flush the remaining rows
                let finalbytes = tokio::task::spawn_blocking(move || {
                    owned.close().expect("closing writer failed?");
                    buff.flush_empty()
                })
                .await
                .expect("got JoinError while finalizing bytestream");
                yield Ok(finalbytes);
            }
        };
        Box::pin(s)
    }
}

impl Stream for TableApiStream {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut me = Pin::new(self);
        let stream = &mut me.stream;
        stream.poll_next_unpin(cx)
    }
}
impl RecordBatchStream for TableApiStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
struct TableApiStreamLimit {
    schema: SchemaRef,
    inner: BatchStreamType,
    limit: usize,
    total: usize,
}
impl Stream for TableApiStreamLimit {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.total >= self.limit {
            return Poll::Ready(None);
        }
        let mut me = Pin::new(self);
        let stream = &mut me.inner;
        let poll = stream.poll_next_unpin(cx);
        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                let new_total = batch.num_rows() + me.total;
                me.total = new_total;
                if new_total < me.limit {
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    let to_ignore = new_total - me.limit;
                    let to_slice = batch.num_rows() - to_ignore;
                    Poll::Ready(Some(Ok(batch.slice(0, to_slice))))
                }
            }
            p => p,
        }
    }
}
impl RecordBatchStream for TableApiStreamLimit {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
/// Specify a set of block numbers
#[derive(Debug)]
pub enum BlockNumSet<'a> {
    Range(u64, u64),
    Numbers(&'a [u64]),
}

impl<'a> BlockNumSet<'a> {
    pub fn from_nums(nums: &'a [u64]) -> Self {
        Self::Numbers(nums)
    }
    pub fn iter(&self) -> BlockNumIter {
        match self {
            BlockNumSet::Range(start, end) => BlockNumIter {
                nums: None,
                cur: 0,
                range_iter: Some(*start..*end),
            },
            BlockNumSet::Numbers(nums) => BlockNumIter {
                cur: 0,
                nums: Some(nums),
                range_iter: None,
            },
        }
    }
    pub fn len(&self) -> usize {
        match self {
            BlockNumSet::Range(start, end) => (end - start) as usize,
            BlockNumSet::Numbers(n) => n.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn owned(&self) -> OwnedBlockNumSet {
        match self {
            BlockNumSet::Range(start, end) => OwnedBlockNumSet::Range(*start, *end),
            BlockNumSet::Numbers(nums) => OwnedBlockNumSet::Numbers(nums.to_vec()),
        }
    }
    /// Divide into `n` sets.
    pub fn chunks(&self, n: usize) -> Vec<Self> {
        if n == 0 {
            warn!(
                "trying to divide into zero chunks! \
                returning empty array but this is probably a logic error"
            );
            return vec![];
        }
        match self {
            BlockNumSet::Range(start, end) => {
                let chunksize = (end - start) / n as u64;
                if chunksize == 0 {
                    return vec![];
                }
                (*start..*end)
                    .step_by((end - start) as usize / n)
                    .map(|start_chunk| {
                        let end_chunk = u64::min(start_chunk + chunksize, *end);
                        Self::Range(start_chunk, end_chunk)
                    })
                    .collect()
            }
            BlockNumSet::Numbers(nums) => {
                let chunksize = nums.len() / n;
                if chunksize == 0 {
                    return vec![];
                }
                let mut iters = Vec::with_capacity(n);
                for i in 0..n {
                    let start = chunksize * i;
                    let end = usize::min(start + chunksize, nums.len());
                    iters.push(BlockNumSet::Numbers(&nums[start..end]));
                }
                iters
            }
        }
    }
}
impl<'a> From<Range<u64>> for BlockNumSet<'a> {
    fn from(value: Range<u64>) -> Self {
        Self::Range(value.start, value.end)
    }
}
impl<'a> From<&'a [u64]> for BlockNumSet<'a> {
    fn from(value: &'a [u64]) -> Self {
        Self::Numbers(value)
    }
}
/// Owned version of `BlockNumSet`
#[derive(Debug)]
pub enum OwnedBlockNumSet {
    Range(u64, u64),
    Numbers(Vec<u64>),
}
impl OwnedBlockNumSet {
    pub fn as_set(&self) -> BlockNumSet {
        match self {
            OwnedBlockNumSet::Range(start, end) => BlockNumSet::Range(*start, *end),
            OwnedBlockNumSet::Numbers(nums) => BlockNumSet::Numbers(nums),
        }
    }
}
pub struct BlockNumIter<'a> {
    cur: usize,
    nums: Option<&'a [u64]>,
    range_iter: Option<Range<u64>>,
}

impl<'a> Iterator for BlockNumIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.nums, &mut self.range_iter) {
            (None, Some(range)) => range.next(),
            (Some(nums), None) => {
                if self.cur < nums.len() {
                    let val = nums[self.cur];
                    self.cur += 1;
                    Some(val)
                } else {
                    None
                }
            }
            (None, None) | (Some(_), Some(_)) => unreachable!(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::{
        test::{chain_empty_idx, TestChain},
        ChainConf, ChainDef,
    };
    use bytes::Bytes;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use test_case::test_case;

    async fn get_table() -> Arc<dyn TableApi> {
        let chain = Arc::new(TestChain::new(ChainConf {
            partition_index: Some(chain_empty_idx(1).await),
            data_fetch_conf: Some(()),
            ..Default::default()
        }));
        let tables = chain.tables();
        let table = &tables[0];
        Arc::clone(table)
    }

    #[tokio::test]
    async fn test_table_api_stream_base() {
        let table = get_table().await;
        let mut stream = TableApiStream::new(table, &BlockNumSet::Range(0, 1000), 100, None);
        let mut batches = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            batches.push(value);
        }
        assert_eq!(batches.len(), 10);
        assert_eq!(batches[0].num_rows(), 100);
    }
    #[tokio::test]
    async fn test_table_api_stream_limit() {
        let table = get_table().await;
        let mut stream =
            TableApiStream::new(table.clone(), &BlockNumSet::Range(0, 1000), 100, None)
                .with_limit(10);
        let mut batches = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            batches.push(value);
        }
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);

        let mut stream =
            TableApiStream::new(table, &BlockNumSet::Range(0, 1000), 100, None).with_limit(0);
        let mut batches = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            batches.push(value);
        }
        assert_eq!(batches.len(), 0);
    }
    #[tokio::test]
    async fn test_table_api_stream_trait_object_method() {
        let table = get_table().await;
        let mut stream = table.stream_batches(&BlockNumSet::Range(0, 1000), 100, None);
        let mut batches = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            batches.push(value);
        }
        assert_eq!(batches.len(), 10);
        assert_eq!(batches[0].num_rows(), 100);
    }

    #[test_case(300, 100; "small_per_chunk")]
    #[test_case(20_000, 100; "lots_of_small_chunks")]
    #[test_case(50_000, 10_000; "medium_chunks")]
    #[test_case(255, 100; "chunks_remainder")]
    #[test_case(100_000, 100_000; "one_big")]
    #[test_case(100, 1_000_000; "per_chunk_larger_than_total")]
    #[tokio::test]
    async fn test_table_parquet_stream(upper: u64, perchunk: u64) {
        let table = get_table().await;
        let stream = TableApiStream::new(
            Arc::clone(&table),
            &BlockNumSet::Range(0, upper),
            perchunk,
            None,
        );
        let mut stream = stream.into_parquet_bytes(None);
        let mut chunks = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            chunks.push(value);
        }
        assert_eq!(chunks.len() as u64, expected_iters(0, upper, perchunk, 1));
        // read bytes as into parquet arrow reader
        let rdr = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_iter(
            chunks.into_iter().flatten(),
        ))
        .expect("corrupt parquet file");
        let schema = rdr.schema();
        assert_eq!(table.schema().fields, schema.fields);
        let mdata = rdr.metadata();
        assert_eq!(
            mdata.row_groups().len() as u64,
            expected_iters(0, upper, perchunk, 1) - 1,
        );
    }

    #[tokio::test]
    async fn test_empty_parquet() {
        let table = get_table().await;
        let stream = TableApiStream::new(Arc::clone(&table), &BlockNumSet::Range(0, 0), 1, None);
        let mut stream = stream.into_parquet_bytes(None);
        let mut chunks = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            chunks.push(value);
        }
        let rdr = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_iter(
            chunks.into_iter().flatten(),
        ))
        .expect("corrupt parquet file");
        assert_eq!(rdr.metadata().num_row_groups(), 0);
    }
    /// just a helper func for getting the number of expected iterations from [`stream_parquet_bytes`]
    fn expected_iters(cur_block: u64, end_block: u64, per_chunk: u64, groupsize: u64) -> u64 {
        let n = end_block.saturating_sub(cur_block);
        (n / per_chunk / groupsize)
            + (match n % per_chunk {
                0 => 1,
                _ => 2,
            })
    }

    #[test_case(1000, 100, 5; "even_divis")]
    #[test_case(1000, 90, 4; "uneven_divis")]
    #[test_case(1000, 100, 100; "one_group")]
    #[test_case(10_000, 100, 3; "many_groups")]
    #[tokio::test]
    async fn test_table_parquet_batch_per_rowgroup(size: u64, perchunk: u64, pergroup: u64) {
        let table = get_table().await;
        let stream = TableApiStream::new(table, &BlockNumSet::Range(0, size), perchunk, None);
        let mut stream =
            stream.into_parquet_bytes(Some(NonZeroUsize::new(pergroup as usize).unwrap()));
        let mut chunks = vec![];
        while let Some(v) = stream.next().await {
            let value = v.unwrap();
            chunks.push(value);
        }
        let numchunks = chunks.len();
        assert_eq!(
            expected_iters(0, size, perchunk, pergroup),
            numchunks as u64
        );
        let rdr = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_iter(
            chunks.into_iter().flatten(),
        ))
        .expect("corrupt parquet file");
        rdr.metadata().row_group(0);
        assert_eq!(
            ((size as f64 / perchunk as f64) / (pergroup as f64)).ceil() as usize,
            rdr.metadata().num_row_groups()
        );
    }
}
