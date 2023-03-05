//! Traits and datatypes for defining a queryable entity in a chain.
//! An entity represents a logical unit of queryable data, for example in ethereum:
//! - a block
//! - a log
//! - a transaction
//!
//! Each entity maps to a table in the query engine.
use crate::{
    partition_index::TablePartitionIndex,
    table_api::{BlockNumSet, TableApi, TableApiError, TableApiStream},
};
use anyhow::{Error, Result};
use async_trait::async_trait;
use datafusion::arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
        LargeBinaryBuilder, LargeStringBuilder, StringBuilder, TimestampSecondBuilder,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::error::Result as DataFusionResult;
use itertools::Itertools;
use num::BigUint;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::mpsc;

use super::ChainApi;

/// Trait for defining a queryable entity and how its raw data maps into Arrow data.
/// Entities are essentially equivalent to tables: each entity becomes a queryable
/// table.
///
/// Interacting with tables (for example, during DataFusion planning/execution)
/// is mainly done through [TableApi] (blanket impl).
#[async_trait]
pub trait EntityDef: std::fmt::Debug + Send + Sync + Sized + 'static {
    /// Type of the raw pre-transform chain data.
    type RawData: Send + Sync;
    /// name of table, must be unique within each chain. i.e. there can be only one "blocks"
    /// table in ethereum but other chains are allowed to have one.
    const NAME: &'static str;
    /// name of the entity (i.e. table) in the database
    fn entity_name(&self) -> &str {
        Self::NAME
    }

    /// Returns columns in the table. **Invalid column definitions will panic at runtime**
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>>;

    /// The name of the column that represents the number of the block on chain that
    /// this row came from. This must be defined for every table and must point to a valid
    /// column that is:
    /// - non nullable
    /// - should be an int (i.e. its transform must be [TransformDef::Int64])
    ///
    /// **Returning the name of an invalid column here will panic during query execution**
    fn blocknum_partition_col(&self) -> &str;
    /// Get the raw data for the given block numbers. Raw data reference counted so that
    /// it can be cached and shared between functions
    async fn raw_data_with_blocknums(
        &self,
        blocknums: &BlockNumSet,
    ) -> Result<Vec<Arc<Self::RawData>>, Error>;

    /// Uses the defined columns to transform a vector of the raw rpc data
    /// into an Arrow [`RecordBatch`]. Will return errors if data does not match what is
    /// expected according to the columns definition. This includes: null values on non-nullable
    /// columns, invalid length for data types that specify a length (e.g. `FixedBytes`).
    ///
    /// You should not (need to) override this method.
    fn raw_to_arrow(&self, raw_vec: Vec<Arc<Self::RawData>>) -> DataFusionResult<RecordBatch> {
        use ColumnTypeDef::*;

        let cols = self.columns();
        let mut builders = cols
            .into_iter()
            .map(|c| (c.transform.builder(), c))
            .collect_vec();
        for raw in &raw_vec {
            for c in builders.iter_mut() {
                // we can safely downcast these based on the enum variant matched
                // TODO: maybe theres a better way to do this without needing
                //  runtime type casting idk
                let builder = c.0.as_any_mut();
                match &c.1.transform {
                    VarChar { from_raw } => {
                        builder
                            .downcast_mut::<LargeStringBuilder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    U256 { from_raw } => {
                        builder
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .append_option(from_raw(raw).map(|u| u.to_str_radix(10)));
                    }
                    I64 { from_raw } => {
                        builder
                            .downcast_mut::<Int64Builder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    U64 { from_raw } => {
                        builder
                            .downcast_mut::<UInt64Builder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    FixedBytes { from_raw, .. } => {
                        let bytevec = from_raw(raw);
                        let b = builder.downcast_mut::<FixedSizeBinaryBuilder>().unwrap();
                        if let Some(value) = bytevec {
                            b.append_value(value)?;
                        } else {
                            b.append_null();
                        }
                    }
                    Blob { from_raw } => {
                        builder
                            .downcast_mut::<LargeBinaryBuilder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    Bytes { from_raw } => {
                        builder
                            .downcast_mut::<BinaryBuilder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    Timestamp { from_raw } => {
                        builder
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                    Float64 { from_raw } => {
                        builder
                            .downcast_mut::<Float64Builder>()
                            .unwrap()
                            .append_option(from_raw(raw));
                    }
                }
            }
        }
        RecordBatch::try_from_iter_with_nullable(builders.iter_mut().map(|(builder, col)| {
            let array = builder.finish();
            (col.name, array, col.nullable)
        }))
        .map_err(Into::into)
    }
    /// Infer an arrow [Schema] using the columns defined
    fn infer_schema(&self) -> Schema {
        let cols = self.columns();
        let fields: Vec<Field> = cols
            .into_iter()
            .map(|c| Field::new(c.name, c.transform.datatype(), c.nullable))
            .collect();
        Schema::new(fields)
    }
    /// Optional table description
    fn description(&self) -> Option<String> {
        None
    }
    /// Reference to the chain this came from
    fn chain(&self) -> Arc<dyn ChainApi>;
}

/// Define a single column in a table.
#[derive(Debug, Clone)]
pub struct ColumnDef<'a, Raw> {
    pub name: &'a str,
    pub nullable: bool,
    pub transform: ColumnTypeDef<'a, Raw>,
}

type FromRaw<'a, Raw, Out> = fn(&'a Raw) -> Option<Out>;

/// Specifies the datatype of the column as well as how to convert the raw data into the
/// expected data type. This is used to get the Arrow [DataType] for schema generation
/// as well as getting the appropriate Arrow [ArrayBuilder] to use when moving the raw
/// data into the final Arrow arrays.
///
/// All from_raw functions should return None when the expected value cannot be found.
///
/// NOTE: for fixed length data, it will be verified during array construction. In other words,
/// you don't need to include validation logic in your `from_raw` funcs.
#[derive(Debug, Clone)]
pub enum ColumnTypeDef<'a, Raw> {
    VarChar {
        /// extract/transform a string given an instance of `Raw`
        from_raw: FromRaw<'a, Raw, &'a str>,
    },
    /// **NOTE: there is no Arrow uin256 so we represent it in Arrow as a base10 string**
    /// This way in cases where the uint256 is used as an identifier no precision is lost
    /// and they still can still properly identify. In cases where it is used as a number,
    /// the user can cast it as runtime and lose some precision.
    U256 {
        /// extract biguint from instance of raw
        from_raw: FromRaw<'a, Raw, BigUint>,
    },
    I64 {
        /// extract/transform scalar i64 value from an instance of 'Raw`
        from_raw: FromRaw<'a, Raw, i64>,
    },
    U64 {
        from_raw: FromRaw<'a, Raw, u64>,
    },
    Float64 {
        from_raw: FromRaw<'a, Raw, f64>,
    },
    /// Bytestrings of fixed length (`num_bytes`).
    /// - hashes (always 32 bytes)  
    /// - ethereum addresses (20 bytes)
    FixedBytes {
        num_bytes: i32,
        from_raw: FromRaw<'a, Raw, Vec<u8>>,
    },
    /// Variable sized bytestrings (that dont need to use alot of space). For example `ethereum.blocks.nonce`
    Bytes {
        from_raw: FromRaw<'a, Raw, Vec<u8>>,
    },
    /// Large binary blob. Useful for stuff such as
    /// - Contract data
    /// - trace input/output
    Blob {
        from_raw: FromRaw<'a, Raw, Vec<u8>>,
    },
    Timestamp {
        /// Should return a unix timestamp (native type is i64 for arrow second timestamps)
        from_raw: FromRaw<'a, Raw, i64>,
    },
}

impl<'a, T> ColumnTypeDef<'a, T> {
    /// Get the associated Arrow [DataType]
    pub const fn datatype(&self) -> DataType {
        match self {
            Self::VarChar { .. } => DataType::LargeUtf8,
            Self::I64 { .. } => DataType::Int64,
            Self::U64 { .. } => DataType::UInt64,
            Self::U256 { .. } => DataType::Utf8,
            Self::FixedBytes {
                num_bytes: ref n, ..
            } => DataType::FixedSizeBinary(*n),
            Self::Blob { .. } => DataType::LargeBinary,
            Self::Bytes { .. } => DataType::Binary,
            Self::Timestamp { .. } => DataType::Timestamp(TimeUnit::Second, None),
            Self::Float64 { .. } => DataType::Float64,
        }
    }
    /// Createa new Arrow [ArrayBuilder] of the appropriate data type
    pub fn builder(&self) -> Box<dyn ArrayBuilder> {
        match self {
            Self::VarChar { .. } => Box::new(LargeStringBuilder::new()),
            Self::I64 { .. } => Box::new(Int64Builder::new()),
            Self::U64 { .. } => Box::new(UInt64Builder::new()),
            // no uint256 in DataFusion, could look into making a custom one eventually
            // leaving as text so we can manipulate it at runtime possibly.
            Self::U256 { .. } => Box::new(StringBuilder::new()),
            Self::FixedBytes {
                num_bytes: ref n, ..
            } => Box::new(FixedSizeBinaryBuilder::new(*n)),
            Self::Bytes { .. } => Box::new(BinaryBuilder::new()),
            Self::Blob { .. } => Box::new(LargeBinaryBuilder::new()),
            Self::Timestamp { .. } => Box::new(TimestampSecondBuilder::new()),
            Self::Float64 { .. } => Box::new(Float64Builder::new()),
        }
    }
}

// blanket impl allowing entities to be treated as tables
#[async_trait]
impl<T> TableApi for T
where
    T: EntityDef + Debug + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.entity_name()
    }
    fn schema(&self) -> Schema {
        self.infer_schema()
    }
    async fn partition_data(&self) -> Result<Option<TablePartitionIndex>, TableApiError> {
        if let Some(chain) = self.chain().partition_index() {
            chain
                .get_table(self.name())
                .await
                .map_err(TableApiError::DataFetching)
        } else {
            Ok(None)
        }
    }
    async fn max_blocknum(&self) -> Result<Option<u64>, TableApiError> {
        Ok(Some(self.chain().max_blocknum().await?))
    }
    async fn batch_for_blocknums(
        &self,
        blocknums: &BlockNumSet,
    ) -> Result<RecordBatch, TableApiError> {
        let data = self
            .raw_data_with_blocknums(blocknums)
            .await
            .map_err(TableApiError::DataFetching)?;
        let batch = self.raw_to_arrow(data).map_err(TableApiError::DataFusion)?;
        Ok(batch)
    }
    fn col_names(&self) -> Vec<String> {
        let schema = self.schema();
        schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect()
    }
    fn blocknum_col(&self) -> &str {
        let col = self.blocknum_partition_col();
        if !self.col_names().contains(&col.to_string()) {
            panic!("invalid column name: {col}");
        }
        col
    }
    fn stream_batches(
        self: Arc<Self>,
        blocknums: &BlockNumSet,
        blocks_per_batch: u64,
        count_chan: Option<mpsc::UnboundedSender<(u64, u64)>>,
    ) -> TableApiStream {
        TableApiStream::new(self, blocknums, blocks_per_batch, count_chan)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::TestChain;
    use super::*;
    use chrono::prelude::*;
    use datafusion::arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use ethereum_types::{H128, H256};
    use num::Num;
    #[derive(Clone, Debug)]
    struct TestRawData {
        textfield: Option<String>,
        uint256field: String,
        int64field: i64,
        bytesfield: Vec<u8>,
        tsfield: i64,
        f64field: f64,
    }
    #[derive(Debug)]
    struct BaseTestData {
        chain: Arc<dyn ChainApi>,
    }

    #[async_trait]
    impl EntityDef for BaseTestData {
        const NAME: &'static str = "testy";
        fn chain(&self) -> Arc<dyn ChainApi> {
            self.chain.clone()
        }
        fn blocknum_partition_col(&self) -> &str {
            "i64"
        }
        type RawData = TestRawData;
        fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
            let mut v: Vec<ColumnDef<Self::RawData>> = Vec::new();
            v.push(ColumnDef {
                name: "text",
                nullable: false,
                transform: ColumnTypeDef::VarChar {
                    from_raw: |x| x.textfield.as_deref(),
                },
            });
            v.push(ColumnDef {
                name: "u256",
                nullable: false,
                transform: ColumnTypeDef::U256 {
                    from_raw: |x| BigUint::from_str_radix(&x.uint256field, 10).ok(),
                },
            });
            v.push(ColumnDef {
                name: "i64",
                nullable: false,
                transform: ColumnTypeDef::I64 {
                    from_raw: |x| Some(x.int64field),
                },
            });
            v.push(ColumnDef {
                name: "u64",
                nullable: false,
                transform: ColumnTypeDef::U64 {
                    from_raw: |x| Some(x.int64field as u64),
                },
            });
            v.push(ColumnDef {
                name: "bytesfield",
                nullable: false,
                transform: ColumnTypeDef::FixedBytes {
                    num_bytes: 32,
                    from_raw: |x| Some(x.bytesfield.clone()),
                },
            });
            v.push(ColumnDef {
                name: "ts",
                nullable: true,
                transform: ColumnTypeDef::Timestamp {
                    from_raw: |x| Some(x.tsfield),
                },
            });
            v.push(ColumnDef {
                name: "f",
                nullable: true,
                transform: ColumnTypeDef::Float64 {
                    from_raw: |x| Some(x.f64field),
                },
            });
            v.push(ColumnDef {
                name: "bigbytes",
                nullable: true,
                transform: ColumnTypeDef::Blob {
                    from_raw: |x| {
                        Some({
                            let mut bytevec: Vec<u8> = Vec::new();
                            for _ in 0..100 {
                                bytevec.append(&mut x.bytesfield.clone())
                            }
                            bytevec
                        })
                    },
                },
            });
            v
        }

        async fn raw_data_with_blocknums(
            &self,
            _nums: &BlockNumSet,
        ) -> Result<Vec<Arc<TestRawData>>, Error> {
            unreachable!()
        }
    }

    fn testdata() -> TestRawData {
        TestRawData {
            bytesfield: H256::random().as_bytes().to_vec(),
            f64field: 1.0,
            textfield: Some("hi".to_string()),
            uint256field:
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                    .to_string(),
            tsfield: Utc::now().timestamp(),
            int64field: i64::MAX,
        }
    }

    async fn test_table() -> BaseTestData {
        BaseTestData {
            chain: Arc::new(TestChain::init().await),
        }
    }

    #[tokio::test]
    async fn test_raw_to_arrow() {
        let raw = (0..100).map(|_| testdata()).map(Arc::new).collect_vec();
        let table = test_table().await;
        let batch = table.raw_to_arrow(raw).unwrap();
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), table.infer_schema().fields.len());
    }

    #[tokio::test]
    async fn test_schema() {
        let table = test_table().await;
        let colnames = table.columns().iter().map(|c| c.name).collect_vec();
        let s = table.infer_schema();
        assert_eq!(s.fields.len(), 8);
        let schemafields = s.fields.iter().map(|f| f.name().as_str()).collect_vec();
        assert_eq!(schemafields, colnames);
        // check ordering
    }

    #[tokio::test]
    async fn nulls_cause_err() {
        let mut raw = (0..100).map(|_| testdata()).collect_vec();
        raw[0].textfield = None;
        let raw = raw.into_iter().map(Arc::new).collect_vec();
        let table = test_table().await;
        let res = table.raw_to_arrow(raw);
        match res {
            Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(_))) => {}
            Err(_) => panic!(),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn invalid_byte_len_cause_errs() {
        let mut raw = (0..100).map(|_| testdata()).collect_vec();
        raw[0].bytesfield = H128::random().as_bytes().to_vec();
        let raw = raw.into_iter().map(Arc::new).collect_vec();
        let table = test_table().await;
        let res = table.raw_to_arrow(raw);
        match res {
            Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(_))) => {}
            Err(_) => panic!(),
            _ => panic!(),
        }
    }
    #[derive(Debug)]
    struct DuplicateCols {
        chain: Arc<TestChain>,
    }
    #[async_trait]
    impl EntityDef for DuplicateCols {
        type RawData = TestRawData;
        const NAME: &'static str = "testy";

        fn blocknum_partition_col(&self) -> &str {
            "i64"
        }
        fn chain(&self) -> Arc<dyn ChainApi> {
            self.chain.clone()
        }
        fn columns(&self) -> Vec<ColumnDef<TestRawData>> {
            vec![
                ColumnDef {
                    name: "text",
                    nullable: false,
                    transform: ColumnTypeDef::VarChar {
                        from_raw: |x| x.textfield.as_deref(),
                    },
                },
                ColumnDef {
                    name: "text",
                    nullable: false,
                    transform: ColumnTypeDef::VarChar {
                        from_raw: |x| x.textfield.as_deref(),
                    },
                },
            ]
        }

        async fn raw_data_with_blocknums(
            &self,
            _nums: &BlockNumSet,
        ) -> Result<Vec<Arc<TestRawData>>, Error> {
            unreachable!()
        }
    }
    // TODO: how are duplicate columns handled in datafusion?
    #[tokio::test]
    async fn duplicate_col_names_raw_to_arrow() {
        let raw = (0..100).map(|_| testdata()).map(Arc::new).collect_vec();
        let table = DuplicateCols {
            chain: Arc::new(TestChain::init().await),
        };
        let res = table.raw_to_arrow(raw).unwrap();
        assert_eq!(res.num_columns(), 2);
        // assert!(res.is_err());
    }

    #[tokio::test]
    async fn duplicate_col_names_schema() {
        let table = DuplicateCols {
            chain: Arc::new(TestChain::init().await),
        };
        let s = table.infer_schema();
        assert_eq!(s.all_fields().len(), 2);
    }
    #[derive(Debug)]
    struct EmptyCols {
        chain: Arc<TestChain>,
    }

    #[async_trait]
    impl EntityDef for EmptyCols {
        type RawData = TestRawData;
        const NAME: &'static str = "testy";
        fn blocknum_partition_col(&self) -> &str {
            "i64"
        }
        fn chain(&self) -> Arc<dyn ChainApi> {
            self.chain.clone()
        }
        fn columns(&self) -> Vec<ColumnDef<TestRawData>> {
            vec![]
        }

        async fn raw_data_with_blocknums(
            &self,
            _nums: &BlockNumSet,
        ) -> Result<Vec<Arc<TestRawData>>, Error> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn no_cols_raw_to_arrow_fails() {
        let raw = (0..100).map(|_| testdata()).map(Arc::new).collect_vec();
        let table = EmptyCols {
            chain: Arc::new(TestChain::init().await),
        };
        let res = table.raw_to_arrow(raw);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn no_cols_schema() {
        let table = EmptyCols {
            chain: Arc::new(TestChain::init().await),
        };
        let s = table.infer_schema();
        assert_eq!(s.all_fields().len(), 0);
    }
}
