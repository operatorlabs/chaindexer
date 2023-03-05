use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use object_store::{
    path::Path, Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::AsyncWrite;

/// keep in mem data across lifetime of process so that it's "persistent" like an s3 bucket
static DATA_MAP: Lazy<Mutex<HashMap<Path, StoreVal>>> = Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub struct MemStore {
    path: Path,
    prefix: Path,
}
#[derive(Debug, Clone)]
struct StoreVal {
    bytes: Bytes,
    ts: DateTime<Utc>,
}

impl MemStore {
    /// since our ipfs store only allows puts on a given path, give a `put_path`
    /// to simulate that behavior
    pub fn new(put_path: Path, bucket: &str) -> Self {
        Self {
            path: put_path,
            prefix: Path::parse(bucket).unwrap(),
        }
    }
    /// modifies all keys before putting them in the hashmap
    /// by adding the bucket name to the front
    fn key(&self, key: &Path) -> Path {
        Path::from_iter(self.prefix.parts().chain(key.parts()))
    }

    pub const fn path(&self) -> &Path {
        &self.path
    }

    pub async fn add_part(&self, key: &Path, data: Bytes) -> Result<()> {
        self.put(&self.key(key), data).await?;
        Ok(())
    }

    fn readloc(&self, location: &Path) -> Result<StoreVal> {
        let guard = DATA_MAP.lock();
        guard
            .get(&self.key(location))
            .cloned()
            .ok_or_else(|| Error::NotFound {
                path: location.to_string(),
                source: anyhow!("not found").into(),
            })
    }
}

impl fmt::Display for MemStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemStore")
    }
}
macro_rules! invalidmethod {
    ($method: expr) => {
        return Err(Error::NotSupported {
            source: anyhow::anyhow!("ObjectStore.{} method not valid in MemStore", $method).into(),
        });
    };
}

// TODO: check boudns for range ops. only used in testing rn so NBD
#[async_trait]
impl ObjectStore for MemStore {
    async fn get(&self, location: &Path) -> Result<GetResult> {
        let val = self.readloc(location)?;
        Ok(GetResult::Stream(Box::pin(futures::stream::once(
            async move { Ok(val.bytes.clone()) },
        ))))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let val = self.readloc(location)?;
        Ok(val.bytes.slice(range))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let data = self.readloc(location)?.bytes;
        Ok(ranges
            .iter()
            .map(|range| data.slice(range.to_owned()))
            .collect())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let val = self.readloc(location)?;
        Ok(ObjectMeta {
            location: location.to_owned(),
            last_modified: val.ts,
            size: val.bytes.len(),
        })
    }
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let mut guard = DATA_MAP.lock();
        guard.insert(
            self.key(location),
            StoreVal {
                bytes,
                ts: Utc::now(),
            },
        );
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let location = self.key(location);
        Ok((
            String::new(),
            Box::new(Multi {
                location,
                buf: Vec::with_capacity(0),
                ts: Utc::now(),
            }),
        ))
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        // no-op
        Ok(())
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        invalidmethod!("delete");
    }

    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        invalidmethod!("list");
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        invalidmethod!("list_with_delimiter");
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("copy");
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("rename");
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("copy_if_not_exists");
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        invalidmethod!("rename_if_not_exists");
    }
}

struct Multi {
    location: Path,
    buf: Vec<u8>,
    ts: DateTime<Utc>,
}

impl tokio::io::AsyncWrite for Multi {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        inp: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.buf.extend_from_slice(inp);
        Poll::Ready(Ok(inp.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let bytes = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(0)));
        let mut w = DATA_MAP.lock();
        w.insert(
            self.location.to_owned(),
            StoreVal {
                bytes,
                ts: self.ts.to_owned(),
            },
        );
        Poll::Ready(Ok(()))
    }
}
