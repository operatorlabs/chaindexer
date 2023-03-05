//! Main export is the [`StorageApi`] which implements [`ObjectStore`] which is used
//! for data fusion query engibne. Since  [`StorageApi`] is only used as [`ObjectStore`]
//! trait objects in the context of the query engine, only read methods are supported.
//!
//! The [`StorageApi`] is also used in non-query parts of the codebase such as for
//! creating partition indices.
pub mod conf;
pub mod ipfs;
mod ipfs_objstore;
mod location;
mod memstore;
mod storage_api;

use bytes::Bytes;
pub use conf::*;
use futures::stream::BoxStream;
pub use ipfs::*;
pub use ipfs_objstore::*;
pub use location::Location;
pub use memstore::MemStore;
pub use object_store::path::Path as ObjStorePath;
pub use storage_api::{Persistable, StorageApi, WritePartitionResult};

use async_trait::async_trait;
use object_store::{
    path::Path, Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use std::ops::Range;
use tokio::io::AsyncWrite;

macro_rules! invalidmethod {
    ($method: expr) => {
        Err(Error::NotSupported {
            source: anyhow::anyhow!(
                "StorageApi doesn't support ObjectStores's {} method",
                $method
            )
            .into(),
        })
    };
}

/// Give storage api an object store impl so it can be used in datafusion contexts.
/// Note many methods are not use-able b/c they do not make sense w/ ipfs.
#[async_trait]
impl<T> ObjectStore for StorageApi<T>
where
    T: Send + Sync + Persistable + 'static,
{
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.object_store().get(location).await
    }
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.object_store().get_range(location, range).await
    }
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.object_store().head(location).await
    }

    async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<()> {
        invalidmethod!("put")
    }
    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        invalidmethod!("put_multipart")
    }
    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        invalidmethod!("abort_multipart")
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        invalidmethod!("delete")
    }

    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        invalidmethod!("list")
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        invalidmethod!("list_with_delimiter")
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("copy")
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("rename")
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("copy_if_not_exists")
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("rename_if_not_exists")
    }
}
