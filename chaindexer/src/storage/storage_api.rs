//! Should refactor this shit, perhaps using only `object_store` but the issue with that
//! is alot of the object_storage functionality does not make sense in the ipfs context.
use super::conf::StorageConf;
use super::{memstore::MemStore, IpfsStore, Location};
use crate::util::randbytes;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use log::info;
pub use object_store::path::Path as ObjStorePath;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    ObjectStore,
};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::{any::Any, sync::Arc};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::io::StreamReader;

/// Simple trait that defines how an object can be persisted to a binary blob and then
/// how such a blob can be converted back into the object.
#[async_trait]
pub trait Persistable: Send + Sync + 'static + std::fmt::Debug {
    async fn to_bytes(&self) -> Result<Bytes>;
    async fn from_bytes(v: &Bytes) -> Result<Self>
    where
        Self: Sized;
}

/// supertrait that adds some extra ability ontop of an [`ObjectStore`].
/// This includes methods for performing runtime inflection, which is currently required
/// for the [`StorageApi`] method `write_part`
trait ObjectStoreApi: ObjectStore {
    /// need to runtime downcast for some cases
    fn as_any(&self) -> &(dyn Any + Send + Sync);
    /// gets ref to self as an [`ObjectStore`] trait object
    fn as_object_store(self: Arc<Self>) -> Arc<dyn ObjectStore>;
    fn scheme(&self) -> &str;
}
macro_rules! impl_mapping_trait {
    ($objstore: ident, $scheme: literal) => {
        impl ObjectStoreApi for $objstore {
            fn scheme(&self) -> &str {
                $scheme
            }
            fn as_any(&self) -> &(dyn Any + Send + Sync) {
                self
            }
            fn as_object_store(self: Arc<Self>) -> Arc<dyn ObjectStore> {
                Arc::clone(&(self as Arc<dyn ObjectStore>))
            }
        }
    };
}
// =============== call macro for all stores supported ===============
impl_mapping_trait!(MemStore, "memory");
impl_mapping_trait!(IpfsStore, "ipfs");
impl_mapping_trait!(LocalFileSystem, "file");
impl_mapping_trait!(AmazonS3, "s3");
// TODO: gcp, azure

/// When a partition is successfully written, this is the response.
#[derive(Debug, Clone)]
pub struct WritePartitionResult {
    /// location in the storage layer (could be CID, s3 key, etc...)
    pub loc: Location,
    /// number of bytes
    pub byte_count: Option<u64>,
}

/// Storage for a data mapping/index (e.g  a [`crate::partition_index::TablePartitionIndex`]).
///
/// Wraps an [ObjectStore] and adds some extra functionality.
///
/// `T` is the type of the underlying mapping. mappings will typically need to write and
/// then point to data stored somewhere, which is done thru `write_partition_data`.
/// `T` must implement [`Persistable`] so that it can be persisted to bytes and then loaded
/// back from said bytes.
///
/// Each instance of a [`StorageApi`] represents the persistence layer for a single
/// mapping. This means the mapping is always stored in the same place for any given
/// [`StorageApi`] instance. In other words, while each instance can write as many
/// data partitions at once, only one underlying mapping can be stored by it.
#[derive(Debug)]
pub struct StorageApi<T>
where
    T: Send + Sync + Persistable + 'static,
{
    store: Arc<dyn ObjectStoreApi>,
    /// location of persisted data of type `T`
    map_loc: ObjStorePath,
    /// path prefix for storing the actual partition data (i.e. not the mapping, which is of type `T`).
    ///
    /// _only applicable for certain stores like s3 and file system..._
    parts_prefix: ObjStorePath,
    /// type of data being persisted
    _dt: PhantomData<T>,
    /// underlying conf used to create the struct instance
    conf: StorageConf,
    bucket: Option<String>,
    /// for testing and debugging
    locs_written: tokio::sync::RwLock<HashSet<Location>>,
    /// for testing and debugging
    id: String,
}
impl<T> std::fmt::Display for StorageApi<T>
where
    T: Send + Sync + Persistable + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "StorageApi{{ object_store: {} }}",
            self.object_store()
        ))
    }
}

impl<T> StorageApi<T>
where
    T: Persistable + Send + Sync + 'static,
{
    /// url scheme for registering it as a provider in datafusion.
    /// e.g. `ipfs` or `mem` or `file`
    pub fn scheme(&self) -> &str {
        self.conf.scheme()
    }
    /// "host" for url. for example, if its S3, the host is the bucket name.
    /// this is often just empty (file, memory, ipfs) all have empty hosts since
    pub fn bucket(&self) -> Option<&str> {
        self.conf.bucket()
    }
    /// Get a reference to the configuration object used to generate this instance
    pub fn conf(&self) -> &StorageConf {
        &self.conf
    }

    /// attempts to create an instance given a config object.
    ///
    /// will return errors when the config is determined to be invalid
    pub async fn try_new(conf: StorageConf) -> Result<Self> {
        let locs_written = tokio::sync::RwLock::new(HashSet::new());
        let id = hex::encode(randbytes(4));
        Ok(match &conf {
            StorageConf::Memory { bucket } => {
                // just use a random name for the main data key so that it doesnt
                // conflict with partitions (mem store is a hash map)
                let loc = ObjStorePath::parse(hex::encode(randbytes(32)))?;
                let store = Arc::new(MemStore::new(loc.clone(), bucket));
                Self {
                    store,
                    map_loc: loc,
                    parts_prefix: "".into(),
                    bucket: Some(bucket.to_owned()),
                    conf,
                    _dt: PhantomData,
                    locs_written,
                    id,
                }
            }
            StorageConf::Ipfs(c) => {
                let store = Arc::new(IpfsStore::new(c.clone()));
                // must be in publish mode (aka have an ipns)
                // to be able to persist a mapping to
                let loc = store.ipns_as_obj_store_path().await?;
                Self {
                    store,
                    map_loc: loc,
                    parts_prefix: "".into(),
                    conf,
                    bucket: None,
                    _dt: PhantomData,
                    locs_written,
                    id,
                }
            }
            StorageConf::File { dirpath, filename } => {
                let dirpath = tokio::fs::canonicalize(dirpath).await?;
                let loc = ObjStorePath::from_absolute_path(
                    dirpath.join(std::path::Path::new(&filename)),
                )?;
                let prefix = ObjStorePath::from_filesystem_path(dirpath).unwrap();
                let store = Arc::new(LocalFileSystem::new());
                Self {
                    store,
                    map_loc: loc,
                    parts_prefix: prefix,
                    conf,
                    bucket: None,
                    _dt: PhantomData,
                    locs_written,
                    id,
                }
            }
            StorageConf::S3 {
                bucket,
                prefix,
                filename,
            } => {
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(true)
                    .build()?;
                let prefix = match prefix {
                    Some(prefix) => ObjStorePath::parse(prefix),
                    None => ObjStorePath::parse(""),
                }?;
                let filename = ObjStorePath::parse(filename)?;
                let map_loc = ObjStorePath::from_iter(prefix.parts().chain(filename.parts()));
                Self {
                    store: Arc::new(store),
                    map_loc,
                    parts_prefix: prefix,
                    bucket: Some(bucket.to_owned()),
                    conf,
                    locs_written,
                    _dt: PhantomData,
                    id,
                }
            }
        })
    }
    /// for debuggin
    pub fn id(&self) -> &str {
        &self.id
    }
    /// load the mapping from the underlying storage and return it. returns None if there is
    /// no mapping saved yet.
    pub async fn load(&self) -> Result<Option<T>> {
        let result: object_store::GetResult = match self.store.get(&self.map_loc).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e.into());
            }
        };
        let raw = result.bytes().await?;
        Ok(Some(T::from_bytes(&raw).await?))
    }
    /// save the mapping to the underlying storage
    pub async fn save(&self, data: &T) -> Result<()> {
        self.store
            .put(&self.map_loc, data.to_bytes().await?)
            .await?;
        Ok(())
    }
    pub async fn locs_written(&self) -> HashSet<Location> {
        let rdr = self.locs_written.read().await;
        rdr.to_owned()
    }
    /// Get a ref to the underlying [`ObjectStore`] trait object
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store).as_object_store()
    }
    /// Writes a new partition.
    /// `part_id` identifies the partition but does not necessarily map
    /// to the actual path in the underlying store. for example, in ipfs it will be the cid.
    pub async fn write_partition<S>(&self, part_id: &str, stream: S) -> Result<WritePartitionResult>
    where
        S: TryStream + Send + 'static,
        // should usually be anyhow
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
        S::Ok: Send,
        Bytes: From<S::Ok>,
    {
        let dynstore = self.store.as_any();
        let host = self.bucket.as_deref();
        let mut keyswritten = self.locs_written.write().await;
        if let Some(store) = dynstore.downcast_ref::<IpfsStore>() {
            let filename = part_id.replace('/', "-");
            // convert the stream to be `Sync` b/c for some reason
            // reqwest requires `Sync`
            let (tx, rx) = futures::channel::mpsc::unbounded::<Result<S::Ok, S::Error>>();
            tokio::task::spawn(async move {
                let stream = stream.into_stream();
                pin_mut!(stream);
                while let Some(v) = stream.next().await {
                    tx.unbounded_send(v).expect("unexpected channel failure");
                }
            });
            // stream now meets reqwest trait bounds for Body::wrap_stream
            let sync_stream = rx.into_stream();
            let body = reqwest::Body::wrap_stream(sync_stream);
            let res = store.add_item(&filename, body).await?;
            let loc = store.path_to_obj_store(&res.hash);
            let url = Location::new(store.scheme(), host, loc);
            keyswritten.insert(url.to_owned());
            Ok(WritePartitionResult {
                loc: url,
                byte_count: res.content_length,
            })
        } else {
            let store = &self.store;
            let partpath = ObjStorePath::parse(part_id)?;
            let location =
                ObjStorePath::from_iter(self.parts_prefix.parts().chain(partpath.parts()));
            // TODO: clean up on errors
            let (mpid, writer) = store.put_multipart(&location).await?;
            let url = Location::new(store.scheme(), host, location.clone());
            keyswritten.insert(url.to_owned());
            match self.copy_multipart(stream, writer).await {
                Ok(byte_count) => Ok(WritePartitionResult {
                    loc: url,
                    byte_count: Some(byte_count),
                }),
                Err(err) => {
                    // cleanup multipart
                    info!(
                        "writing partition {part_id} failed so cleaning up multipart upload now..."
                    );
                    store
                        .abort_multipart(&location, &mpid)
                        .await
                        .expect("multipart cleanup failed!");
                    Err(err)
                }
            }
        }
    }
    /// just a helper method for multipart upload
    async fn copy_multipart<S>(
        &self,
        stream: S,
        mut writer: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Result<u64>
    where
        S: TryStream,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<S::Ok>,
    {
        let mut reader =
            Box::pin(StreamReader::new(stream.map_ok(Bytes::from).map_err(
                |err| std::io::Error::new(std::io::ErrorKind::Other, err),
            )));
        let byte_count = tokio::io::copy(&mut reader, &mut writer).await?;
        writer
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!(e).context("writer.shutdown failed!"))?;
        Ok(byte_count)
    }

    /// utility method for writing single byte objects. just wraps the bytes in a stream basically
    pub async fn write_partition_bytes<B>(&self, part_id: &str, bytes: B) -> Result<Location>
    where
        B: Send + Sync + 'static,
        Bytes: From<B>,
    {
        let stream = Box::pin(futures::stream::once(async {
            let result: Result<B, std::io::Error> = Ok(bytes);
            result
        }));
        let res = self.write_partition(part_id, stream).await?;
        Ok(res.loc)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::PathBuf;

    use super::super::ipfs::tests::{ipfs_integration_test, ipfs_store};
    use super::*;
    use crate::chains::test::{chain_empty_idx, TestChain};
    use crate::chains::{ChainConf, ChainDef};
    use crate::table_api::TableApi;
    use crate::test::{integration_test_flag, setup_integration};
    use crate::{partition_index::ChainPartitionIndex, test::TestDir};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use itertools::Itertools;
    async fn memstore() -> StorageApi<ChainPartitionIndex> {
        StorageApi::<ChainPartitionIndex>::try_new(StorageConf::Memory {
            bucket: "testy".to_owned(),
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_storage_try_new() {
        let td = TestDir::new(true);
        let filename = "testy.json".to_string();
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: td.path.clone(),
            filename: filename.to_string(),
        })
        .await
        .unwrap();
        assert_eq!(
            store.parts_prefix,
            ObjStorePath::parse(td.path.to_str().unwrap()).unwrap()
        );
        assert_eq!(
            store.map_loc.parts().last().unwrap(),
            object_store::path::PathPart::parse(&filename).unwrap()
        );
        assert_eq!(memstore().await.parts_prefix, "".into());
    }
    #[tokio::test]
    async fn test_storage_try_new_dirpath_does_not_exist() {
        let td = TestDir::new(false);
        let filename = "testy.json".to_string();
        let err = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: td.path.clone(),
            filename: filename.to_string(),
        })
        .await
        .unwrap_err();
        err.downcast::<std::io::Error>().unwrap();
    }

    #[test]
    fn test_partition_loc() {
        let scheme = "file";
        let bucket = None;
        let loc = ObjStorePath::parse("/var").unwrap();
        let p = Location::new(scheme, bucket, loc);
        assert_eq!(p.bucket(), bucket);
        assert_eq!(p.scheme(), scheme);
        assert_eq!(p.as_str(), "file:///var");
        let loc = ObjStorePath::from_filesystem_path("./").unwrap();
        let p = Location::new(scheme, bucket, loc);
        let s = &p.as_str()[7..];
        assert_eq!(s, env!("CARGO_MANIFEST_DIR"));
        let bucket = "test";
        let loc = ObjStorePath::parse("/var").unwrap();
        let p = Location::new(scheme, Some(bucket), loc);
        assert_eq!(p.bucket().unwrap(), bucket);
        let loc = ObjStorePath::parse("var").unwrap();
        let p = Location::new(scheme, None, loc);
        assert_eq!(p.as_str(), "file:///var");
    }

    #[tokio::test]
    async fn test_store_write_part_fs() {
        let td = TestDir::new(true);
        let filename = "testy.json".to_string();
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: td.path.clone(),
            filename: filename.to_string(),
        })
        .await
        .unwrap();
        store
            .write_partition_bytes("testy", Bytes::from("some data"))
            .await
            .unwrap();
        let dirfile = std::fs::read_dir(&td.path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(dirfile.file_name(), "testy");
        let contents = std::fs::read(dirfile.path()).unwrap();
        assert_eq!(contents, Bytes::from("some data"));
    }
    #[tokio::test]
    async fn test_store_write_stream_fs() {
        let td = TestDir::new(true);
        let filename = "testy.json".to_string();
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: td.path.clone(),
            filename: filename.to_string(),
        })
        .await
        .unwrap();
        let data: Vec<Result<Bytes, anyhow::Error>> = vec![
            Ok(Bytes::from("some data ")),
            Ok(Bytes::from("more data ")),
            Ok(Bytes::from("last piece ok")),
        ];
        let stream = futures::stream::iter(data);
        store.write_partition("testy", stream).await.unwrap();
        let dirfile = std::fs::read_dir(&td.path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(dirfile.file_name(), "testy");
        let contents = std::fs::read(dirfile.path()).unwrap();
        let expected = b"some data more data last piece ok".as_slice();
        assert_eq!(contents.as_slice(), expected);
    }

    #[test]
    fn test_conf_serde_works_as_expected() {
        // just making sure the enum renames and the tag types are working
        let conf = StorageConf::File {
            dirpath: PathBuf::from("/"),
            filename: "testy.json".to_string(),
        };
        let ser = serde_json::to_value(conf).unwrap();
        let obj = ser.as_object().unwrap();
        assert_eq!(obj.get("type").unwrap(), "file");
        assert_eq!(obj.get("dirpath").unwrap(), "/");
        let conf = StorageConf::Memory {
            bucket: "testy".to_owned(),
        };
        let ser = serde_json::to_value(conf).unwrap();
        assert!(ser.as_object().unwrap().get("conf").is_none());
    }

    async fn get_table() -> Arc<dyn TableApi> {
        let chain = Arc::new(TestChain::new(ChainConf {
            partition_index: Some(chain_empty_idx(1).await),
            data_fetch_conf: Some(()),
            // ..Default::default()
        }));
        let tables = chain.tables();
        let table = &tables[0];
        Arc::clone(table)
    }

    /// give a store and it tests that it can  upload a parquet using the table api
    /// and makes sure its not corrupt when it fetches
    macro_rules! test_parquet_with_store {
        ($store:ident) => {{
            let table = get_table().await;
            let bytestream = table
                .stream_batches(&crate::table_api::BlockNumSet::Range(0, 100), 50, None)
                .into_parquet_bytes(NonZeroUsize::new(2));
            let res = $store.write_partition("testy", bytestream).await?;
            let loc = res.loc.to_owned();
            let uploaded_data = $store.object_store().get(&res.loc.into()).await?;
            let bytes = uploaded_data.bytes().await?;
            let rdr =
                ParquetRecordBatchReaderBuilder::try_new(bytes).expect("corrupt parquet file");
            let mdata = rdr.metadata();
            assert_eq!(mdata.row_groups().len(), 1);
            let batches = rdr.build().unwrap().collect_vec();
            let batch = batches[0].as_ref().unwrap();
            assert_eq!(batch.num_rows(), 100);
            loc
        }};
    }
    #[tokio::test]
    async fn test_fs_parquet_stream() -> Result<()> {
        let td = TestDir::new(true);
        let filename = "testy.json".to_string();
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
            dirpath: td.path.clone(),
            filename: filename.to_string(),
        })
        .await
        .unwrap();
        test_parquet_with_store!(store);
        Ok(())
    }
    #[tokio::test]
    async fn test_mem_parquet_stream() -> Result<()> {
        let store = memstore().await;
        test_parquet_with_store!(store);
        Ok(())
    }

    #[test]
    fn test_storage_location_is_valid_file() {
        let c = StorageConf::File {
            dirpath: PathBuf::from("/var/data"),
            filename: "testy.db".to_owned(),
        };
        assert!(c.location_is_valid(&Location::new(
            "file",
            None,
            ObjStorePath::parse("/var/data/testy").unwrap()
        )));
        assert!(c.location_is_valid(&Location::new(
            "file",
            None,
            ObjStorePath::parse("/var/data/testy/deeper/in/the/path").unwrap()
        )));
        assert!(!c.location_is_valid(&Location::new(
            "file",
            None,
            ObjStorePath::parse("/var/bad").unwrap()
        )));
        assert!(!c.location_is_valid(&Location::new(
            "asdf",
            None,
            ObjStorePath::parse("/var/bad").unwrap()
        )));
    }
    #[test]
    fn test_storage_location_is_valid_s3_with_prefix() {
        let c = StorageConf::S3 {
            bucket: "testbucket".to_string(),
            prefix: Some("prefix".to_string()),
            filename: "file.db".to_string(),
        };
        assert!(c.location_is_valid(&Location::new(
            "s3",
            Some("testbucket"),
            ObjStorePath::parse("/prefix/cool").unwrap()
        )));
        assert!(c.location_is_valid(&Location::new(
            "s3",
            Some("testbucket"),
            ObjStorePath::parse("/prefix/file.db").unwrap()
        )));
        assert!(!c.location_is_valid(&Location::new(
            "s3",
            None,
            ObjStorePath::parse("/var/bad").unwrap()
        )));
        assert!(!c.location_is_valid(&Location::new(
            "asdf",
            Some("testbucket"),
            ObjStorePath::parse("/prefix/cool").unwrap()
        )));
    }
    #[test]
    fn test_storage_location_is_valid_s3_no_prefix() {
        let c = StorageConf::S3 {
            bucket: "testbucket".to_string(),
            prefix: None,
            filename: "file.db".to_string(),
        };
        assert!(c.location_is_valid(&Location::new(
            "s3",
            Some("testbucket"),
            ObjStorePath::parse("/prefix/cool").unwrap()
        )));
        assert!(c.location_is_valid(&Location::new(
            "s3",
            Some("testbucket"),
            ObjStorePath::parse("/other_prefix/file.db").unwrap()
        )));
        assert!(c.location_is_valid(&Location::new(
            "s3",
            Some("testbucket"),
            ObjStorePath::parse("file.db").unwrap()
        )));
        assert!(!c.location_is_valid(&Location::new(
            "s3",
            Some("badbucket"),
            ObjStorePath::parse("file.db").unwrap()
        )));
    }
    // ================= integration tests =================

    #[tokio::test]
    async fn integration_test_mapping_store_ipfs() -> Result<()> {
        ipfs_integration_test!();
        let ipfs = ipfs_store().await;
        let conf = StorageConf::Ipfs(ipfs.conf);
        let mapping_store = StorageApi::<ChainPartitionIndex>::try_new(conf).await?;
        let res = test_parquet_with_store!(mapping_store);
        assert_eq!(res.bucket(), None);
        assert_eq!(res.scheme(), "ipfs");
        Ok(())
    }
    #[tokio::test]
    async fn integration_test_mapping_store_s3() -> Result<()> {
        if !integration_test_flag() {
            eprint!("skipping...");
            return Ok(());
        }
        // check for buckets
        setup_integration();
        let bucket = std::env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET not set!");
        std::env::set_var(
            "AWS_ENDPOINT",
            std::env::var("TEST_S3_LOCALSTACK").expect("TEST_S3_LOCALSTACK not set!"),
        );
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        let conf = StorageConf::S3 {
            bucket: bucket.clone(),
            filename: "mapping.json".to_owned(),
            prefix: None,
        };
        let mapping_store = StorageApi::<ChainPartitionIndex>::try_new(conf).await?;
        let res1 = test_parquet_with_store!(mapping_store);
        let conf = StorageConf::S3 {
            bucket: bucket.clone(),
            filename: "mapping.json".to_owned(),
            prefix: Some("prefix".to_owned()),
        };
        assert_eq!(res1.bucket().unwrap(), &bucket);
        assert_eq!(res1.scheme(), "s3");
        assert!(res1.path().to_string().starts_with("testy"));
        let mapping_store = StorageApi::<ChainPartitionIndex>::try_new(conf).await?;
        let res2 = test_parquet_with_store!(mapping_store);
        assert!(res2.path().to_string().starts_with("prefix"));
        assert_eq!(res2.bucket().unwrap(), &bucket);
        assert_eq!(res2.scheme(), "s3");
        Ok(())
        // Ok(())
    }
}
