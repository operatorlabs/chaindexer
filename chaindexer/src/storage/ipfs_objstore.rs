//! [`ObjectStore`] impl over the ipfs api
//! **NOTE:** Alot of methods the [`ObjectStore`] methods will return [`Error::NotSupported`].
//! This is because they are not applicable to the [`ObjectStore`] api. But we still need  
//! to impl the api so we can use it in certain contexts where we know the not supported
//! methods will not be called. as an object store in some contexts.
//!
//! Using the [`ObjectStore`] api only requires the store to be in [`IpfsMode::Read`],
//! (in other words, an IPFS kubo daemon is not required.)
//!
//! For flexibiltiy with the rest of the codebase, for get/head requests, locations
//! are tried as both IPFS cids **and** IPNS names. See `storage_api` for why this makes sense.
use super::ipfs::{IpfsApiErr, IpfsStore};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path, Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use reqwest::Response;
use std::ops::Range;
use tokio::io::AsyncWrite;
/// just a lil macro for returning [`ObjectStore`]'s [`Error::NotSupported`] given
/// a method name
macro_rules! invalidmethod {
    ($method: expr) => {
        Err(Error::NotSupported {
            source: anyhow::anyhow!("ObjectStores's {} method not valid in IpfsStore", $method)
                .into(),
        })
    };
}
/// lil macro for returning a not found error
macro_rules! notfounderr {
    ($loc: expr) => {
        Err(Error::NotFound {
            path: $loc.to_string(),
            source: anyhow!("location not found as neither ipfs cid nor ipns name").into(),
        })
    };
}
/// generate ipfs/ipns urls to check
fn loc_to_urls(store: &IpfsStore, loc: &Path) -> [String; 2] {
    [
        // try it as an ipfs path first
        format!("{}/ipfs/{}", store.conf.gateway_url, loc),
        format!("{}/ipns/{}", store.conf.gateway_url, loc),
    ]
}

#[async_trait]
impl ObjectStore for IpfsStore {
    async fn get(&self, location: &Path) -> Result<GetResult> {
        // location as ipfs and then ipns if it 404s
        let urls = loc_to_urls(self, location);
        let mut res: Option<Response> = None;
        for u in urls {
            let tryres = self
                .gateway_client
                .get(u)
                .send()
                .await
                .map_err(map_reqwest_err_to_object_store)?;
            if tryres.status().as_u16() < 400 {
                res = Some(tryres);
                break;
            }
        }
        res.map_or_else(
            || notfounderr!(location),
            |res| {
                let stream = res
                    .bytes_stream()
                    .map_err(|source| Error::Generic {
                        store: "ipfs",
                        source: Box::new(source),
                    })
                    .boxed();
                Ok(GetResult::Stream(stream))
            },
        )
    }
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        if range.start >= range.end {
            // invalid range
            return Ok(Bytes::new());
        }
        let urls = loc_to_urls(self, location);
        let mut res: Option<Response> = None;
        for u in urls {
            let tryres = self
                .gateway_client
                .get(u)
                .header(
                    "Range",
                    format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
                )
                .send()
                .await
                .map_err(map_reqwest_err_to_object_store)?;
            if tryres.status().as_u16() < 400 {
                res = Some(tryres);
                break;
            }
        }
        if let Some(res) = res {
            let data = res.bytes().await.map_err(|err| Error::Generic {
                store: "ipfs",
                source: err.into(),
            })?;
            Ok(data)
        } else {
            notfounderr!(location)
        }
    }
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let urls = loc_to_urls(self, location);
        let mut res: Option<Response> = None;
        for u in urls {
            let tryres = self
                .gateway_client
                .head(u)
                .send()
                .await
                .map_err(map_reqwest_err_to_object_store)?;
            if tryres.status().as_u16() < 400 {
                res = Some(tryres);
                break;
            }
        }
        if let Some(res) = res {
            let headers = res.headers();
            let content_length = headers
                .get("content-length")
                .map(|h| {
                    h.to_str()
                        .expect("somehow failed to convert header to string")
                })
                .unwrap_or("0");
            let size = usize::from_str_radix(content_length, 10).map_err(|_| Error::Generic {
                store: "ipfs",
                source: anyhow!("Got invalid content-length {content_length}").into(),
            })?;
            Ok(ObjectMeta {
                location: location.clone(),
                // TODO
                last_modified: Utc::now(),
                size,
            })
        } else {
            notfounderr!(location)
        }
    }
    /// this one is weird b/c it doesnt work like puts for all the other object store
    /// impls. this is b/c in [`IpfsStore`] we can only "put" to one location (the one
    /// pointed to via ipns). the only reason we really implement it here at
    /// all is to simplify the storage api and allow it to just wrap an object store.
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        // we want this to only work in the case where its writing to its publishable name
        let keyid = self.key_id().await.map_err(map_api_err)?;
        let expected_loc = self.path_to_obj_store(&keyid);
        if location != &expected_loc {
            return Err(Error::NotSupported {
                source: anyhow!("cannot add data to any location other than this stores ipns name")
                    .into(),
            });
        }
        // first post the data to ipfs
        let add_res = self
            .add_item("ipfs_store_item", bytes)
            .await
            .map_err(map_api_err)?;
        let cid = add_res.hash;
        // publish cid to our ipns name
        self.ipns_publish(&cid).await.map_err(map_api_err)?;
        Ok(())
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

/// wrap reqwest send errors into [IpfsApiErr]
fn map_reqwest_send_err(err: reqwest::Error) -> IpfsApiErr {
    match err.is_timeout() {
        true => IpfsApiErr::RequestTimeout,
        false => IpfsApiErr::HttpRequestFailed(err),
    }
}
/// wrap a reqwest error into our IpfsApiErr and then wrap that in
/// [ObjectStore]'s error, in order to conform to the trait
fn map_reqwest_err_to_object_store(err: reqwest::Error) -> Error {
    Error::Generic {
        store: "ipfs",
        source: map_reqwest_send_err(err).into(),
    }
}

fn map_api_err(err: anyhow::Error) -> Error {
    Error::Generic {
        store: "ipfs",
        source: err.into(),
    }
}
