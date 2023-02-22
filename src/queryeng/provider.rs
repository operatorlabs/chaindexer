use super::ctx::CtxStateRef;
use crate::{
    partition_index::{BlockPartition, TablePartitionIndex},
    // queryeng::exec_plan::PartitionIndexExecPlan,
    table_api::{BlockNumSet, OwnedBlockNumSet, TableApi, TableRef},
};
use anyhow::anyhow;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, UInt64Array},
        compute::SortOptions,
        datatypes::SchemaRef,
        record_batch::RecordBatch,
    },
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        get_statistics_with_limit,
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        MemTable, TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::{SessionState, TaskContext},
    logical_expr::{
        expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
        LogicalPlan, TableProviderFilterPushDown, TableType,
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        empty::EmptyExec,
        file_format::{FileScanConfig, ParquetExec},
        project_schema, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
    prelude::{lit, Column, Expr, SessionContext},
    scheduler::Scheduler,
};
use futures::TryStreamExt;
use futures::{future, stream::StreamExt};
use itertools::Itertools;
use log::{debug, info, warn};
use object_store::ObjectStore;
use once_cell::sync::{Lazy, OnceCell};
use std::{any::Any, collections::HashSet, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
struct IndexExec {
    inner: Option<Arc<ParquetExec>>,
    projected: SchemaRef,
}
impl IndexExec {
    fn as_execution_plan(&self) -> Arc<dyn ExecutionPlan> {
        match &self.inner {
            Some(plan) => plan.clone(),
            None => Arc::new(EmptyExec::new(false, self.projected.clone())),
        }
    }
}

/// Wrapper around the [`TableApi`] that implements DataFusion's [TableProvider] trait.
pub struct ChaindexerTableProvider {
    table: Arc<dyn TableApi>,
    partition_index: Option<TablePartitionIndex>,
    schema: SchemaRef,
    state: CtxStateRef,
    opts: Opts,
    /// For helper logic that runs on arrow data, we use a local DataFusion context
    /// to do the required computations.
    helper_ctx: Lazy<Arc<SessionContext>>,
    /// Scheduler manages a threadpool for running DataFusion computations.
    ///
    /// (DataFusion scheduler is basically a small wrapper around a rayon threadpool).
    helper_scheduler: OnceCell<Arc<Scheduler>>,
    helper_sem: OnceCell<Arc<Semaphore>>,
}

#[derive(Clone, Debug)]
/// Options for how some helper functions behave (for example [`run_filters_range_batches`]).
pub struct Opts {
    /// For helper funcs, the max amount of chunks that can be processed concurrently
    /// at any given time.
    pub helpers_max_concurrent_chunks: usize,
    /// Number of threads to use for running helper logic.
    pub threadpool_size: usize,
    /// Target partition count for the actual query engine (not our helper code)
    pub query_engine_target_partitions: usize,
}
/// When filtering partitions, how many to do at a time
const PARTITIONS_PER_CHUNK: usize = 5; // TODO: tune and make configurable
impl Default for Opts {
    fn default() -> Self {
        // TODO: tune these defaults
        Self {
            helpers_max_concurrent_chunks: 64,
            threadpool_size: std::thread::available_parallelism().unwrap().get(),
            query_engine_target_partitions: 64,
        }
    }
}
/// lil macro that generates a closure that takes an arbitrary error, adds anyhow context and converts
/// into a [`DataFusionError`] (datafusion traits require this error type)
macro_rules! map_err {
    ($ctx: expr) => {
        |e| DataFusionError::External(anyhow!(e).context($ctx).into())
    };
}
/// Get object store url for [`TablePartitionIndex`].
///
/// # NOTE:
/// This assumes that all partitions in an index were stored with the same storage api
/// which is a reasonable assumption and that will be enforced in the [`TablePartitionIndex`]
/// code (that is a TODO)
async fn get_objstore_url(index: &TablePartitionIndex) -> Result<ObjectStoreUrl> {
    let part = index
        .last_partition()
        .await
        .map_err(map_err!(
            "failed to load partition while trying to calculate object store url"
        ))?
        .ok_or(DataFusionError::External(
            anyhow!("no partitions exist in index").into(),
        ))?;
    Ok(part.location.into())
}

/// create one filter out of several by anding them
fn merge_filters(filters: &[Expr]) -> Expr {
    filters
        .iter()
        .fold(lit(true), |combo, expr| combo.and(expr.to_owned()))
}

impl ChaindexerTableProvider {
    pub async fn try_create(table: Arc<dyn TableApi>, state: CtxStateRef) -> anyhow::Result<Self> {
        // let conf = FileScanConfig {};
        let schema = table.schema();
        let partition_index = table.partition_data().await?;
        if partition_index.is_some() {
            info!("loaded partition index for {}", table.name());
        }
        Ok(Self {
            // TODO: allow customizing the parquet options
            table,
            schema: Arc::new(schema),
            partition_index,
            state,
            opts: Default::default(),
            helper_ctx: Lazy::new(|| {
                debug!("initializing datafusion context for helper funcs");
                Arc::new(SessionContext::new())
            }),
            helper_scheduler: OnceCell::new(),
            helper_sem: OnceCell::new(),
        })
    }

    fn scheduler(&self) -> &Arc<Scheduler> {
        self.helper_scheduler
            .get_or_init(|| Arc::new(Scheduler::new(self.opts.threadpool_size)))
    }
    /// Perform table scan in selective query mode.
    ///
    /// Selective query mode implies a fixed range of blocks numbers are being queried.
    /// If there is not a fixed range, this will error out!
    async fn scan_selective_mode(
        &self,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _df_state: &SessionState,
    ) -> Result<Arc<SelectiveExec>> {
        // make configurable
        let target_partitions = self.opts.threadpool_size;
        let prunable_exprs = filters
            .iter()
            .filter(|e| expr_is_pruner(e, self.table.blocknum_col()))
            .collect_vec();
        let (start_block, end_block) = if prunable_exprs.is_empty() {
            warn!(
                "no pruning can be done in selective mode. falling back to state to \
                 hard coded block range"
            );
            let start = self.state.start_block().ok_or_else(|| {
                DataFusionError::Execution(
                    "No start_block in state. Selective query mode requires \
                     a bounded block range!"
                        .to_string(),
                )
            })?;
            let end = self.state.end_block().ok_or_else(|| {
                DataFusionError::Execution(
                    "No end_block in state. Selective query mode requires \
                     a bounded block range!"
                        .to_string(),
                )
            })?;
            (start, end)
        } else {
            let max_block = if let Some(block) = self.state.end_block() {
                block
            } else {
                self.table
                    .max_blocknum()
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?
                    .ok_or_else(|| {
                        DataFusionError::Execution("No max blocknumber could be found!".to_string())
                    })?
            };
            let start_block = self.state.start_block().unwrap_or(0);
            (start_block, max_block)
        };
        let range = end_block - start_block;
        let partsize = range as usize / target_partitions;
        let colname = self.table.blocknum_col();
        // get individual blocks
        let df_futs = (start_block..end_block)
            .step_by(partsize)
            .map(|i| (i, u64::min(i + partsize as u64, end_block)))
            .map(|(start, end)| rec_batch_range_u64(colname, start, end))
            .map(|batch| {
                let ctx = self.helper_ctx.clone();
                let filters = filters.to_owned();
                async {
                    tokio::task::spawn_blocking(move || {
                        let memtable = MemTable::try_new(batch.schema(), vec![vec![batch]])
                            .expect("should always be able to create a table in memory");
                        let ctx = ctx.clone();
                        // create data frame using the memtable and apply all the filters
                        filters.iter().fold(
                            ctx.read_table(Arc::new(memtable)).expect(
                                "should read memory table into temporary datafusion context",
                            ),
                            |df, filter| {
                                df.filter(filter.clone())
                                    .expect("failed to apply filter to dataframe")
                            },
                        )
                    })
                    .await
                    .expect("blocking task shouldnt panic")
                }
            })
            .collect_vec();

        let dfs = future::join_all(df_futs).await;
        let df_results = future::try_join_all(dfs.into_iter().map(|df| {
            let ctx_clone = self.helper_ctx.clone();
            let sched = self.scheduler().clone();
            async move {
                let task = ctx_clone.task_ctx();
                let plan = df.create_physical_plan().await?;
                let results = sched.schedule(plan, task)?;
                let filtered_data: Vec<RecordBatch> = results.stream().try_collect().await?;
                Ok(filtered_data) as Result<Vec<RecordBatch>>
            }
        }))
        .await?;
        let blocknumbers = df_results
            .into_iter()
            .flat_map(|batches| {
                batches.into_iter().flat_map(|batch| {
                    let col = batch.column(0);
                    let intcol = col
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("should've been uint64 array!");
                    intcol
                        .iter()
                        .map(|v| v.expect("should be no nulls in block number computation cols"))
                        .collect_vec()
                })
            })
            .collect_vec();
        let blockset = BlockNumSet::from_nums(&blocknumbers);
        let chunks = blockset
            .chunks(target_partitions)
            .into_iter()
            .map(|v| v.owned())
            .collect();
        Ok(Arc::new(SelectiveExec::new(
            self.table.clone(),
            self.state.clone(),
            chunks,
            limit,
        )))
    }

    /// perform the scan operation agaisnt a partition index
    async fn scan_partition_index(
        &self,
        index: TablePartitionIndex,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        df_state: &SessionState,
    ) -> Result<IndexExec> {
        let name = self.table.name();
        let total_partitions = index.num_partitions().await.map_err(map_err!(format!(
            "table '{name}' failed to calc total_partitions"
        )))?;
        info!(
            "preparing logical table scan on '{name}'. \
             partition_count: {total_partitions}"
        );
        // TODO: make configurable

        let partsize = index.blocks_per_partition();
        let blocknum_col = index.blocknum_column();
        let projected = project_schema(&self.schema, projection)?;
        if total_partitions == 0 {
            info!("no partitions existed for table '{name}'!");
            return Ok(IndexExec {
                inner: None,
                projected: projected.clone(),
            });
        }
        let first_part = index
            .first_partition()
            .await
            .map_err(map_err!("partition look up failed unexpectedly"))?
            .expect("atleast one partition should've existed");
        let object_store_url: ObjectStoreUrl = first_part.location.clone().into();
        let store_api = self
            .state
            .chain_store_for_loc(&first_part.location)
            .await
            .map_err(map_err!(
                "error while resolving object store using first partition from index"
            ))?;
        let obj_store = store_api.object_store();

        // filter out partitions
        let partitions: Vec<BlockPartition> = index
            .stream_block_partitions(self.opts.helpers_max_concurrent_chunks)
            .map_err(|e| DataFusionError::External(e.into()))
            .and_then(move |parts| {
                self.filter_block_partitions(filters, blocknum_col, partsize, parts)
            })
            .try_fold(vec![], |mut acc, parts| async move {
                acc.extend(parts.into_iter());
                Ok(acc)
            })
            .await?;
        // create stream of file/stats tuples and feed into datafusion `get_statistics_with_limit` fn
        let tuple_stream = futures::stream::iter(partitions.into_iter()).then(|partition| {
            let obj_store = obj_store.clone();
            async move {
                let stats = self
                    .fetch_partition_stats(&partition, &obj_store, df_state)
                    .await?;
                Ok((partition.as_object_meta().into(), stats))
                    as Result<(PartitionedFile, Statistics)>
            }
        });
        // pipe stream to compute stats over all partitions
        let (partitions, statistics) =
            get_statistics_with_limit(tuple_stream, self.schema.clone(), limit).await?;
        if partitions.is_empty() {
            info!("filtered out all partitions for table '{name}'");
            return Ok(IndexExec {
                inner: None,
                projected: projected.clone(),
            });
        }
        let num_pruned = total_partitions - partitions.len();
        debug!(
            "pruned {num_pruned} partitions: total was {total_partitions}, \
             now {} partitions left to query",
            partitions.len()
        );
        // TODO: validate partition schemas (should probably store serialized schema in the partition index)
        let file_groups: Vec<Vec<PartitionedFile>> = partitions
            .chunks(
                (partitions.len() + self.opts.query_engine_target_partitions - 1)
                    / self.opts.query_engine_target_partitions,
            )
            .map(|chunk| chunk.to_vec())
            .collect();
        // TODO: register object store
        let filescan_config = FileScanConfig {
            object_store_url,
            file_schema: self.schema.clone(),
            file_groups,
            statistics,
            projection: projection.cloned(),
            limit,
            table_partition_cols: vec![],
            // TODO: tell query engine that its sorted by the blocknum_col?
            output_ordering: None,
            infinite_source: false,
        };
        // combine filters
        let combo = filters
            .iter()
            .fold(lit(true), |combo, expr| combo.and(expr.to_owned()));
        let plan = ParquetExec::new(filescan_config, Some(combo), None);
        Ok(IndexExec {
            inner: Some(Arc::new(plan)),
            projected,
        })
    }
    /// helper func that runs the number of rows in a [`BlockPartition`] after applying `filters`
    ///
    /// returns a tuple of the count of rows after filtering and the partition id
    async fn block_partition_filtered_row_count(
        &self,
        partition: &BlockPartition,
        filters: &[Expr],
        blocks_per_partition: u64,
        blocknum_col: &str,
    ) -> Result<(usize, u64)> {
        // acquire semaphore for helper logic
        let start = partition.lower_bound.to_owned();
        let end = start + blocks_per_partition;
        let ctx = self.helper_ctx.clone();
        let sched = self.scheduler().clone();
        let colname = blocknum_col.to_owned();
        let filts = filters.iter().cloned().collect_vec();
        let ctx_clone = ctx.clone();
        // bunch of blocking stuff like generating record batches
        // and creating in memory tables in datafusion
        // TODO: maybe for really large partitions this can be split up idk
        let df = tokio::task::spawn_blocking(move || {
            // construct an in memory datafusion table using the ranged batches
            // and evaluate the filters against the table in an in memory context.
            // since we're only looking at filters can be evaluated only using
            // `blocknum_col`, the filters can be run against this table
            let batch = rec_batch_range_u64(&colname, start, end);
            let memtable = MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("should always be able to create a table in memory");
            let ctx = ctx.clone();
            // create data frame using the memtable and apply all the filters
            filts.iter().fold(
                ctx.read_table(Arc::new(memtable))
                    .expect("should read memory table into temporary datafusion context"),
                |df, filter| {
                    df.filter(filter.clone())
                        .expect("failed to apply filter to dataframe")
                },
            )
        })
        .await
        .expect("join blocking task failed unexpectedly");
        // compute results on final dataframe...
        let task = ctx_clone.task_ctx();
        let plan = df.create_physical_plan().await?;
        let results = sched.schedule(plan, task)?;
        // let stream = results.stream();
        // let batch: Vec<RecordBatch> = results.stream().
        let filtered_data: Vec<RecordBatch> = results.stream().try_collect().await?;
        let rowcount = filtered_data
            .iter()
            .fold(0, |acc, cur| acc + cur.num_rows());
        Ok((rowcount, partition.lower_bound))
    }

    /// Fetch the [`Statistics`] for a `partition`.
    async fn fetch_partition_stats(
        &self,
        partition: &BlockPartition,
        store: &Arc<dyn ObjectStore>,
        df_state: &SessionState,
    ) -> Result<Statistics> {
        let format = ParquetFormat::new().with_enable_pruning(Some(true));
        let obj = partition.as_object_meta();
        // TODO: look into setting size hint
        // format.with_metadata_size_hint(size_hint)
        format
            .infer_stats(df_state, store, self.schema.clone(), &obj)
            .await
    }

    async fn filter_block_partitions(
        &self,
        filters: &[Expr],
        blocknum_col: &str,
        blocks_per_partition: u64,
        partitions: Vec<BlockPartition>,
    ) -> Result<Vec<BlockPartition>> {
        let filters = filters
            .iter()
            .filter(|e| expr_is_pruner(e, blocknum_col))
            .cloned()
            .collect_vec();
        if filters.is_empty() {
            debug!("no blocknumber filtering can be done with: {filters:?}");
            return Ok(partitions);
        }
        println!("combined filters: {}", merge_filters(&filters));
        // for each partition, run the filters on a sub table generated
        // by taking the range of blocknumbers and count the rows after filtering
        let results: Vec<(usize, u64)> = future::try_join_all(partitions.iter().map(|p| {
            self.block_partition_filtered_row_count(
                p,
                filters.as_slice(),
                blocks_per_partition,
                blocknum_col,
            )
        }))
        .await?;

        let ids = results
            .into_iter()
            .fold(HashSet::new(), |mut set, (rowcount, id)| {
                if rowcount == 0 {
                    set
                } else {
                    set.insert(id);
                    set
                }
            });
        Ok(partitions
            .into_iter()
            .filter(|v| ids.contains(&v.lower_bound))
            .collect_vec())
    }
}

#[async_trait]
impl TableProvider for ChaindexerTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let name = self.table.name();
        match self.table.partition_data().await {
            Err(e) => Err(DataFusionError::External(
                anyhow!(e)
                    .context(format!(
                        "table '{name}' failed to load partitioned table from backing index"
                    ))
                    .into(),
            )),
            Ok(Some(index)) => {
                let plan = self
                    .scan_partition_index(index, projection, filters, limit, ctx)
                    .await?;
                let execplan = plan.as_execution_plan();
                Ok(execplan)
            }
            Ok(None) => {
                info!(
                    "table {} has no partition index, assuming selective query mode",
                    self.table.name()
                );
                let plan = self
                    .scan_selective_mode(projection, filters, limit, ctx)
                    .await?;
                Ok(plan)
            }
        }
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> Result<TableProviderFilterPushDown> {
        // TODO: handle other types of tables besids a block partitioned table
        Ok(if expr_is_pruner(filter, self.table.blocknum_col()) {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        })
    }

    fn get_table_definition(&self) -> Option<&str> {
        // TODO
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        // TODO
        None
    }

    fn statistics(&self) -> Option<Statistics> {
        self.partition_index.as_ref().map(|p| p.stats())
    }
}

fn rec_batch_range_u64(column: &str, lower: u64, upper: u64) -> RecordBatch {
    let arr = UInt64Array::from_iter(lower..upper);
    RecordBatch::try_from_iter_with_nullable(std::iter::once((
        column,
        Arc::new(arr) as ArrayRef,
        false,
    )))
    .expect("should always be able to create RecordBatch from range")
}

/// Can `expr` used to do partition pruning? (i.e. partitioning in which data is
/// always partitioned based on a column `blocknum_col`)
pub fn expr_is_pruner(expr: &Expr, partition_col: &str) -> bool {
    struct PruningVisitor<'a> {
        col_name: &'a str,
        is_valid: &'a mut bool,
    }
    impl ExpressionVisitor for PruningVisitor<'_> {
        fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
            let rec = match expr {
                Expr::Column(Column { ref name, .. }) => {
                    if self.col_name != name {
                        // hit column thats not a partition col
                        *self.is_valid = false;
                        Recursion::Stop(self)
                    } else {
                        Recursion::Continue(self)
                    }
                }
                Expr::Literal(_)
                | Expr::Alias(_, _)
                | Expr::ScalarVariable(_, _)
                | Expr::Not(_)
                | Expr::IsNotNull(_)
                | Expr::IsNull(_)
                | Expr::IsTrue(_)
                | Expr::IsFalse(_)
                | Expr::IsUnknown(_)
                | Expr::IsNotTrue(_)
                | Expr::IsNotFalse(_)
                | Expr::IsNotUnknown(_)
                | Expr::Negative(_)
                | Expr::Cast { .. }
                | Expr::TryCast { .. }
                | Expr::BinaryExpr { .. }
                | Expr::Between { .. }
                | Expr::Like { .. }
                | Expr::ILike { .. }
                | Expr::SimilarTo { .. }
                | Expr::InList { .. }
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
                | Expr::ScalarSubquery(_)
                | Expr::GetIndexedField { .. }
                | Expr::GroupingSet(_)
                | Expr::ScalarFunction { .. }
                | Expr::Case { .. } => Recursion::Continue(self),
                // =================================================================
                // =========== cant prune any expression involving these =========== 
                // =================================================================
                Expr::Placeholder {/*TODO: this is a newly added variant in v16 need to read more about it */  .. }
                // aggs dont let us split up the filtering across ranges
                | Expr::AggregateUDF { .. }
                | Expr::AggregateFunction { .. }
                // TODO: scalar udfs are doable if we have access to them
                | Expr::ScalarUDF { .. }
                | Expr::Sort { .. }
                // no idea how window functions would work for this
                | Expr::WindowFunction { .. }
                | Expr::Wildcard
                | Expr::QualifiedWildcard { .. } => {
                    *self.is_valid = false;
                    Recursion::Stop(self)
                }
            };
            Ok(rec)
        }
    }
    let mut valid = true;
    let vis = PruningVisitor {
        col_name: partition_col,
        is_valid: &mut valid,
    };
    expr.accept(vis).unwrap();
    valid
}
fn nums_to_ranges(nums: &[u64]) -> Vec<std::ops::Range<u64>> {
    let mut ranges = vec![];
    let mut cur: Option<std::ops::Range<u64>> = None;
    for n in nums {
        let num = *n;
        match cur {
            Some(mut range) if range.end == num => {
                range.end += 1;
                cur = Some(range);
            }
            Some(range) => {
                // push last range onto list
                ranges.push(range);
                // start new range
                cur = Some(std::ops::Range {
                    start: num,
                    end: num + 1,
                })
            }
            None => {
                cur = Some(std::ops::Range {
                    start: num,
                    end: num + 1,
                });
            }
        }
    }
    if let Some(range) = cur {
        ranges.push(range);
    }
    ranges
}

/// SelectiveExec is an execution plan that only works on selective queries--i.e. queries
/// that touch a specified subset of block numbers. This subset is parsed before this
/// node is initialized.
#[derive(Debug)]
pub struct SelectiveExec {
    /// Table that this execution node uses to resolve data.
    table: TableRef,
    state: CtxStateRef,
    schema: SchemaRef,
    partitions: Vec<OwnedBlockNumSet>,
    sort_expr: Vec<PhysicalSortExpr>,
    limit: Option<usize>,
}
impl SelectiveExec {
    /// Create new execution plan with partitions as vector of (start_block, end_block)
    pub fn new(
        table: TableRef,
        state: CtxStateRef,
        partitions: Vec<OwnedBlockNumSet>,
        limit: Option<usize>,
    ) -> Self {
        use datafusion::physical_plan::expressions::col;
        let schema = Arc::new(table.schema());
        let sort_expr = vec![PhysicalSortExpr {
            expr: col(table.blocknum_col(), &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        Self {
            schema: Arc::new(table.schema()),
            table,
            sort_expr,
            partitions,
            state,
            limit,
        }
    }
}

impl ExecutionPlan for SelectiveExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.sort_expr)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.partitions.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())));
        }
        let blocknums = &self.partitions[partition];
        let table = self.table.clone();
        let mut stream =
            table.stream_batches(&blocknums.as_set(), self.state.blocks_per_batch(), None);
        if let Some(lim) = self.limit {
            stream = stream.with_limit(lim);
        }
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        // statistics not known ahead of time
        Statistics {
            // TODO: would be knowable for some entities (e.g. `blocks`)
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::eth::test::get_rpc_url;
    use crate::chains::{ChainConf, EthChain, EthDynConf};
    use crate::queryeng::test::{build_test_chain_with_index, TestChainOpts};
    use crate::test::integration_test_flag;
    use crate::util::RpcApiConfig;
    use crate::{
        chains::{
            test::{empty_chain_table, TestEntity},
            EntityDef,
        },
        queryeng::ctx::Ctx,
    };
    use datafusion::logical_expr::{col, lit, BuiltinScalarFunction};
    use std::ops::Div;

    async fn provider_ctx() -> (ChaindexerTableProvider, Ctx) {
        let ctx = Ctx::new();
        let provider = ChaindexerTableProvider::try_create(empty_chain_table(), ctx.state())
            .await
            .unwrap();
        (provider, ctx)
    }

    fn empty_block_partitions(table: &str, n: usize, partsize: u64) -> Vec<BlockPartition> {
        (0..n)
            .map(|i| BlockPartition {
                table: table.to_owned(),
                lower_bound: (i as u64) * partsize,
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn test_expr_is_blocknum_pruner_no_col() {
        let a = lit(51_i32).gt_eq(lit(50_i32));
        assert!(expr_is_pruner(&a, "blocknum"));
    }
    #[test]
    fn test_expr_is_blocknum_pruner_one_col() {
        let a = col("blocknum")
            .gt_eq(lit(51_i32))
            .and(col("blocknum").lt_eq(lit(600)));
        assert!(expr_is_pruner(&a, "blocknum"));
    }
    #[test]
    fn test_expr_is_blocknum_pruner_multi_col() {
        let a = col("blocknum")
            .gt_eq(lit(51_i32))
            .and(col("not_blocknum").lt_eq(lit(600)));
        assert!(!expr_is_pruner(&a, "blocknum"));
    }
    #[test]
    fn test_expr_is_blocknum_pruner_sql_scalars() {
        let f = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Floor,
            args: vec![col("blocknum"), lit(100)],
        };
        assert!(expr_is_pruner(&f, "blocknum"));
    }

    #[test]
    fn test_nums_to_range() {
        let nums = vec![0, 1, 2, 6, 7, 100, 101];
        let ranges = nums_to_ranges(&nums);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], 0..3);
        assert_eq!(ranges[2], 100..102);
        let nums = vec![];
        let ranges = nums_to_ranges(&nums);
        assert_eq!(ranges.len(), 0);
        let nums = vec![0];
        let ranges = nums_to_ranges(&nums);
        assert_eq!(ranges[0], 0..1);
    }

    #[tokio::test]
    async fn test_filter_partitions() {
        // test floor(blocknum/100) > 0
        let (prov, _) = provider_ctx().await;
        let colname = prov.table.blocknum_col();
        let f = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Floor,
            args: vec![col(colname).div(lit(100))],
        };
        let partsize = 100;
        let filt = f.gt(lit(0));
        let parts = empty_block_partitions(prov.table.name(), 5, partsize);
        let res = prov
            .filter_block_partitions(&vec![filt], colname, partsize, parts)
            .await
            .unwrap();
        let ids = res.iter().fold(HashSet::new(), |mut set, part| {
            set.insert(part.lower_bound);
            set
        });
        assert_eq!(ids.len(), 4);
        assert!(!ids.contains(&0));
    }

    #[tokio::test]
    async fn test_filtered_partitions_one_batch() {
        let (prov, _) = provider_ctx().await;
        let colname = prov.table.blocknum_col();
        let f = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Floor,
            args: vec![col(colname).div(lit(100))],
        };
        let partsize = 10_000;
        let parts = empty_block_partitions(prov.table.name(), 1, partsize);
        let filt = f.gt(lit(0));
        let res = prov
            .filter_block_partitions(&vec![filt], colname, partsize, parts)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
    }

    #[tokio::test]
    async fn test_partition_filter_multi_filter() {
        let (prov, _) = provider_ctx().await;
        let colname = prov.table.blocknum_col();
        let filt = col(colname).gt_eq(lit(100));
        let filt2 = col(colname).gt_eq(lit(400));
        let part = BlockPartition {
            lower_bound: 0,
            ..Default::default()
        };
        let (count, _) = prov
            .block_partition_filtered_row_count(
                &part,
                &vec![filt.clone(), filt2.clone()],
                1000,
                colname,
            )
            .await
            .unwrap();
        assert_eq!(count, 600);
        let combined = merge_filters(&vec![filt.clone(), filt2.clone()]);
        let (count, _) = prov
            .block_partition_filtered_row_count(&part, &vec![combined], 1000, colname)
            .await
            .unwrap();
        assert_eq!(count, 600);
    }

    #[tokio::test]
    async fn test_filter_partitions_or() {
        let (prov, _) = provider_ctx().await;
        let colname = prov.table.blocknum_col();
        let partsize = 100;
        // 1000 blocks
        let parts = empty_block_partitions(prov.table.name(), 10, partsize);
        // greater than 900 or less than 100
        let filt = vec![col(colname).gt(lit(900)).or(col(colname).lt(lit(100)))];
        let parts = prov
            .filter_block_partitions(&filt, colname, partsize, parts)
            .await
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().map(|p| p.lower_bound).contains(&0));
    }

    #[tokio::test]
    async fn test_table_qscan_partition_index() {
        let index = build_test_chain_with_index(TestChainOpts {
            end_block: 100_00,
            // store: TestStore::File,
            blocks_per_partition: 100,
            ..Default::default()
        })
        .await
        .unwrap();
        println!("built partition index");
        let ctx = Ctx::new();

        let chain = index.chain.clone();
        let store = index.store;
        ctx.register_chain(chain.clone());
        ctx.add_storage_conf("store", store.conf()).await.unwrap();
        index
            .chain
            .clone()
            .partition_index()
            .unwrap()
            .all_tables()
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .clone()
            .first_partition()
            .await
            .unwrap()
            .unwrap();
        let table = ctx
            .catalog()
            .get_chain(chain.name())
            .unwrap()
            .table_api(TestEntity::NAME)
            .unwrap();
        let table_idx = table.partition_data().await.unwrap().unwrap();
        // assert_eq!(table_idx.num_partitions().await.unwrap(), 100);
        let colname = table_idx.blocknum_column();
        let prov = ChaindexerTableProvider::try_create(table, ctx.state())
            .await
            .unwrap();
        let filt = vec![col(colname).gt(lit(900)).or(col(colname).lt(lit(100)))];
        let _res = prov
            .scan_partition_index(table_idx, None, &filt, None, &ctx.ctx().state())
            .await
            .unwrap();
    }
    pub fn setup_rpc_integration() {
        crate::test::setup_integration();
        let required_vars = vec!["TEST_ETH_RPC_URL"];
        for v in required_vars {
            if std::env::var(v).is_err() {
                panic!("reuqired environment var {v} not found!");
            }
        }
    }
    /// return early if integration test flag not on. make sure env vars are defined otherwise
    macro_rules! maybe_check_rpc_exists_env {
        () => {
            if integration_test_flag() {
                eprintln!("integration tests are turned on... proceeding with setup");
                setup_rpc_integration();
            } else {
                eprintln!("skipping integration test...");
                // return early
                return Ok(());
            }
        };
    }
    #[tokio::test]
    async fn test_selective_mode_filter_exprs() -> anyhow::Result<()> {
        use crate::chains::{ChainApi, ChainDef};
        let target_parts = std::thread::available_parallelism().unwrap().get();
        maybe_check_rpc_exists_env!();
        let conf = RpcApiConfig {
            url: Some(get_rpc_url()),
            batch_size: Some(100),
            max_concurrent: Some(10),
            ..Default::default()
        };
        // create eth chain with no partition index
        let eth_chain = Arc::new(EthChain::new(ChainConf {
            partition_index: None,
            data_fetch_conf: Some(EthDynConf { rpc: conf }),
        }));
        let tables = eth_chain.clone().get_tables();
        let table = tables
            .iter()
            .find(|t| t.name() == "blocks")
            .unwrap()
            .clone();
        let current_block = eth_chain.max_blocknum().await?;
        // do table scan
        let ctx = Ctx::new();
        let provider = ChaindexerTableProvider::try_create(table.clone(), ctx.state())
            .await
            .unwrap();

        let exec_plan = provider
            .scan(
                &ctx.ctx().state(),
                None,
                &vec![col(table.blocknum_col()).gt(lit(current_block - 100))],
                None,
            )
            .await?;
        let selective_plan = exec_plan.as_any().downcast_ref::<SelectiveExec>().unwrap();
        assert_eq!(selective_plan.partitions.len(), target_parts);
        assert_eq!(
            selective_plan.partitions[0].as_set().len(),
            100 / target_parts
        );
        Ok(())
    }
}
