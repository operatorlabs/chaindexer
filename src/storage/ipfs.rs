use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::{debug, error, info};
use object_store::ObjectStore;
use reqwest::{multipart, Body, Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, time::Duration};
use thiserror::Error;

/// interface for interacting with IPFS.
///
/// `rpc_client` is an http client that is connected to a Kubo (the standard
/// ipfs command line impl) daemon.
///
///
/// if there is no `rpc_client` then it runs in [`IpfsMode::Read`] using a gateway.
#[derive(Debug)]
pub struct IpfsStore {
    pub rpc_client: Option<Client>,
    pub gateway_client: Client,
    pub conf: IpfsStoreConf,
    pub mode: IpfsMode, // api: IpfsApi,
    /// name of the key registered in the ipfs kubo daemon
    pub keyname: Option<String>,
}

/// serializable config for creating ipfs stores
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct IpfsStoreConf {
    /// ipfs gateway endpoint for read operations
    ///
    /// TODO: figure out how to do reads using `rpc_api_url` so that gateway is only required
    /// when `rpc_api_url` is null
    pub gateway_url: String,
    /// rpc api endpoint for the ipfs daemon. required for write operations.
    /// typically this will be bound to your localhost and running on port 5001
    pub rpc_api_url: Option<String>,
    /// path on local filesystem to a **ed25519 private key** for publishing to IPNS.
    /// the key will automatically be imported into the ipfs daemon (using `rpc_api_url`)
    pub key_path: Option<String>,
    /// name of a key that has already been imported (for example via `ipfs key import <name> <key>`).
    /// **this will override `key_path` if both are defined**
    pub key_name: Option<String>,
    pub rpc_timeout_ms: u64,
    pub gateway_timeout_ms: u64,
    /// when data is uploaded, by default we pin it to the ipfs node. aka we make
    /// sure the file stays available.
    pub skip_pin: Option<bool>,
}

/// Custom error. For [ObjectStore]'s [Error], these underlying errors can be obtained
/// by downcasting `source`.
#[derive(Debug, Error)]
pub enum IpfsApiErr {
    #[error("IpfsStore is not in the correct mode to use this method")]
    MethodNotSupported,
    #[error("keyname is not registered in ipfs daemon")]
    InvalidKeyname,
    #[error("request to ipfs api failed")]
    HttpRequestFailed(reqwest::Error),
    #[error("request to ipfs api timed out")]
    RequestTimeout,
    #[error("got http error response code (400+)")]
    HttpErrorResponseCode(Response), // { res: Response },
    #[error("failed to deserialize json payload returned by ipfs")]
    PayloadDeserializationFailed(reqwest::Error),
    #[error("unexpected error occurred")]
    Unexpected(reqwest::Error),
    #[error("got unexpected value in response")]
    UnexpectedResponse(String),
}
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IpfsMode {
    /// can only do reads with gateway
    Read,
    /// can add objects to IPFS. being able to add implies reads too
    Add,
    /// can add objects and publish to an IPNS name. note that publish implies it can add and read
    Publish,
}
impl fmt::Display for IpfsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read => write!(f, "Read"),
            Self::Add => write!(f, "Add"),
            Self::Publish => write!(f, "Publish"),
        }
    }
}

impl fmt::Display for IpfsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IpfsStore<mode={}>", self.mode,)
    }
}

impl IpfsStore {
    /// create a new store instance.
    pub fn new(conf: IpfsStoreConf) -> Self {
        let gateway_client = Client::builder()
            .timeout(Duration::from_millis(conf.gateway_timeout_ms))
            .build()
            .unwrap();

        match conf.rpc_api_url.as_ref() {
            None => Self {
                mode: IpfsMode::Read,
                keyname: None,
                rpc_client: None,
                gateway_client,
                conf,
            },
            Some(_) => {
                let rpc_client = Client::builder()
                    .timeout(Duration::from_millis(conf.rpc_timeout_ms))
                    .build()
                    .unwrap();
                // TODO: validate key name here perhaps
                let keyname = conf.key_name.clone();
                Self {
                    mode: match keyname {
                        Some(_) => IpfsMode::Publish,
                        None => IpfsMode::Add,
                    },
                    keyname,
                    rpc_client: Some(rpc_client),
                    gateway_client,
                    conf,
                }
            }
        }
    }
    /// get id of the key associated with this instance
    pub async fn key_id(&self) -> Result<String> {
        if self.mode != IpfsMode::Publish {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let allkeys = self.list_keys().await?;
        let matching = allkeys
            .into_iter()
            .find(|k| &k.name == self.keyname.as_ref().unwrap());
        match matching {
            None => Err(anyhow!(IpfsApiErr::InvalidKeyname)),
            Some(k) => Ok(k.id),
        }
    }
    /// get this stores ipns location as an object store path
    pub async fn ipns_as_obj_store_path(&self) -> Result<object_store::path::Path> {
        let keyid = self.key_id().await?;
        Ok(self.path_to_obj_store(&keyid))
    }

    /// list all the keys imported into kubo connected to at `conf.rpc_api_url`   
    pub async fn list_keys(&self) -> Result<Vec<IpfsRpcKey>> {
        if self.mode == IpfsMode::Read {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let client = self.rpc_client.as_ref().unwrap();
        let url = format!(
            "{}/api/v0/key/list",
            &self.conf.rpc_api_url.as_ref().unwrap()
        );
        let res = client
            .post(url)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        let res = self.check_status(res)?;
        let data = res
            .json::<Value>()
            .await
            .map_err(IpfsApiErr::PayloadDeserializationFailed)?;
        match data {
            Value::Object(obj) => {
                let keys = obj.get("Keys").ok_or(IpfsApiErr::UnexpectedResponse(
                    "expected to find Keys in json response payload".to_string(),
                ))?;
                let keyobjs: Vec<IpfsRpcKey> =
                    serde_json::from_value(keys.clone()).map_err(|_| {
                        IpfsApiErr::UnexpectedResponse(
                            "could not deserialize value from field Keys".to_string(),
                        )
                    })?;
                Ok(keyobjs)
            }
            _ => Err(anyhow!(IpfsApiErr::UnexpectedResponse(
                "got invalid json payload (expected an object)".to_string(),
            ))),
        }
    }

    pub async fn create_key(&self, name: &str) -> Result<IpfsRpcKey> {
        if self.mode == IpfsMode::Read {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let client = self.rpc_client.as_ref().unwrap();
        let url = format!(
            "{}/api/v0/key/gen?arg={name}",
            &self.conf.rpc_api_url.as_ref().unwrap()
        );
        let res = client
            .post(url)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        let res = self.check_status(res)?;
        res.json::<IpfsRpcKey>()
            .await
            .map_err(|e| anyhow!(IpfsApiErr::PayloadDeserializationFailed(e)))
    }
    /// change the key being used for this isntance of the store.
    ///
    /// note that the key's validity will only checked lazily (i.e. the next time
    /// a request that requires using the key is performed)
    fn set_key(&mut self, new_keyname: &str) {
        self.keyname = Some(new_keyname.to_string());
        self.mode = IpfsMode::Publish;
    }

    /// publish a CID to the store's IPNS addy (equivalent to the id pointed to by `key_name`)
    pub async fn ipns_publish(&self, cid: &str) -> Result<()> {
        if self.mode != IpfsMode::Publish {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let client = self.rpc_client.as_ref().unwrap();
        let url = format!(
            "{}/api/v0/name/publish?arg={cid}&key={}&allow-offline=true",
            &self.conf.rpc_api_url.as_ref().unwrap(),
            self.keyname.as_ref().unwrap()
        );
        let res = client
            .post(url)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        self.check_status(res)?;
        Ok(())
    }

    fn check_status(&self, res: Response) -> Result<Response> {
        if res.status().as_u16() >= 400 {
            Err(anyhow!(IpfsApiErr::HttpErrorResponseCode(res)))
        } else {
            Ok(res)
        }
    }

    /// get the cid that `ipns_name` (equivalent to the `id` field of [`IpfsRpcKey`]) points to .
    pub async fn ipns_resolve(&self, ipns_name: &str) -> Result<String> {
        let u = format!("{}/ipns/{}", self.conf.gateway_url, ipns_name);
        let res = self
            .gateway_client
            .head(u)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        let rootval = res.headers().get("x-ipfs-roots").ok_or_else(|| {
            IpfsApiErr::UnexpectedResponse(
                "expected to find key x-ipfs-roots in response header from ipfs gateway"
                    .to_string(),
            )
        })?;
        Ok(rootval
            .to_str()
            .expect("failed to parse value in header at key x-ipfs-roots")
            .to_string())
    }

    /// like ipns_resolve except it returns the data at the ipfs path that the ipns
    /// name points to
    pub async fn ipns_get(&self, ipns_name: &str) -> Result<Bytes> {
        let u = format!("{}/ipns/{}", self.conf.gateway_url, ipns_name);
        let res = self
            .gateway_client
            .get(u)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        let res = self.check_status(res)?;
        Ok(res.bytes().await.expect("failed to read response as bytes"))
    }

    /// Sort of like [`ObjectStore`]'s put but doesnt take a location (filename is not location
    /// its more like metadata).
    /// **note**: the CID of the newly added item is contained in [`IpfsAddResponse`]'s
    /// `hash` field.
    pub async fn add_item<T>(&self, filename: &str, content: T) -> Result<IpfsAddResponse>
    where
        T: Into<Body> + Send + Sync,
    {
        if self.mode == IpfsMode::Read {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let client = self.rpc_client.as_ref().unwrap();
        let url = format!("{}/api/v0/add", &self.conf.rpc_api_url.as_ref().unwrap());
        let reqpart = multipart::Part::stream(content.into())
            .file_name(filename.to_owned())
            .mime_str("application/octet-stream")
            .map_err(IpfsApiErr::Unexpected)?;
        let form = multipart::Form::new().part(filename.to_owned(), reqpart);
        let res = client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        let res = self.check_status(res)?;
        let mut data = res
            .json::<IpfsAddResponse>()
            .await
            .map_err(IpfsApiErr::PayloadDeserializationFailed)?;
        let cid = &data.hash;
        info!("new item successfully added to ipfs. filename={filename} cid={cid}");
        if self.conf.skip_pin.is_none() {
            self.pin_item(cid).await.map_err(|err| {
                let msg = err.to_string();
                err.context(format!("failed to pin item {cid} due to: {msg}"))
            })?;
        }
        let objstore_path = self.path_to_obj_store(cid);
        debug!("fetching items stats w/ HEAD request");
        let head = self.head(&objstore_path).await.map_err(|e| {
            anyhow!(e).context("ipfs upload succeeded but could not make HEAD request to new item")
        })?;
        data.content_length = Some(head.size as u64);
        Ok(data)
    }

    /// pins an item via the IPFS rpc api
    pub async fn pin_item(&self, cid: &str) -> Result<()> {
        if self.rpc_client.is_none() {
            return Err(anyhow!(IpfsApiErr::MethodNotSupported));
        }
        let client = self.rpc_client.as_ref().unwrap();
        let url = format!(
            "{}/api/v0/pin/add?arg={}",
            &self.conf.rpc_api_url.as_ref().unwrap(),
            cid
        );
        let res = client
            .post(url)
            .json(&Value::String(cid.to_string()))
            .send()
            .await
            .map_err(map_reqwest_send_err)?;
        self.check_status(res)?;
        Ok(())
    }
    /// TODO: custom CID datatype
    pub fn path_to_obj_store(&self, cid: &str) -> object_store::path::Path {
        object_store::path::Path::parse(cid).unwrap()
    }
}

impl Default for IpfsStoreConf {
    fn default() -> Self {
        Self {
            rpc_api_url: Some("http://127.0.0.1:5001".to_owned()),
            key_path: None,
            key_name: None,
            rpc_timeout_ms: 60_000,
            gateway_timeout_ms: 60_000,
            skip_pin: None,
            // TODO: find a good gateway to default to
            gateway_url: "http://127.0.0.1:8080".to_owned(),
        }
    }
}

/// response payload returned from add operations
#[derive(Debug, Deserialize)]
pub struct IpfsAddResponse {
    #[serde(rename = "Hash")]
    pub hash: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Size")]
    pub size: String,
    pub content_length: Option<u64>,
}
/// response payload returned from add operations
#[derive(Debug, Deserialize)]
pub struct IpfsRpcKey {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
}
/// wrap reqwest send errors into [IpfsApiErr]
fn map_reqwest_send_err(err: reqwest::Error) -> IpfsApiErr {
    match err.is_timeout() {
        true => IpfsApiErr::RequestTimeout,
        false => IpfsApiErr::HttpRequestFailed(err),
    }
}

#[cfg(test)]
pub mod tests {
    use crate::test::{randbytes, setup_integration};
    use anyhow::Result;
    use bytes::Bytes;
    use itertools::Itertools;
    use object_store::{Error, ObjectStore};
    use test_case::test_case;

    use super::{IpfsStore, IpfsStoreConf};

    /// name of the keypair. TODO: make configurable
    pub(crate) const TEST_IPFS_KEYPAIR: &str = "testy_ipfs_keypair";
    /// get store for testing. panics if it cant find a test url.
    /// sets up a keypair for testing if it cant find one
    ///
    // TODO: use defaults during testing instead
    // of panicking if not found in env
    pub(crate) async fn ipfs_store() -> IpfsStore {
        setup_integration();
        let rpc_api_url = Some(std::env::var("TEST_IPFS_API").unwrap());
        let gateway_url = std::env::var("TEST_IPFS_GATEWAY").unwrap();
        let store = IpfsStore::new(IpfsStoreConf {
            gateway_url,
            rpc_api_url,
            key_name: Some(TEST_IPFS_KEYPAIR.to_string()),
            ..Default::default()
        });
        let keys = store
            .list_keys()
            .await
            .expect("failed to list keys during setup");
        if !keys
            .into_iter()
            .map(|k| k.name)
            .contains(&TEST_IPFS_KEYPAIR.to_string())
        {
            // create key with test name
            store
                .create_key(TEST_IPFS_KEYPAIR)
                .await
                .expect("Failed to create test key!");
        };

        store
    }
    /// skip a test if integration flag is not turned on. if it is, make sure
    /// that the ipfs config vars are found.
    macro_rules! ipfs_integration_test {
        () => {
            if $crate::test::integration_test_flag() {
                eprintln!("integration tests are turned on... proceeding with setup");
                $crate::test::setup_integration();
                for v in ["TEST_IPFS_API", "TEST_IPFS_GATEWAY"] {
                    if std::env::var(v).is_err() {
                        panic!("{} expected to be in environment for testing", v);
                    }
                }
            } else {
                eprintln!("skipping integration test...");
                // return early
                return Ok(());
            }
        };
    }
    pub(crate) use ipfs_integration_test;
    // =============== integration tests =================

    /// upload bytes to the the test ipfs and then return them and their cid
    async fn testybytes(n: usize) -> (Vec<u8>, String) {
        let store = ipfs_store().await;
        let randbytes = randbytes(n);
        let res = store
            .add_item("testy", Bytes::copy_from_slice(&randbytes))
            .await
            .unwrap();
        (randbytes.to_vec(), res.hash)
    }
    #[tokio::test]
    async fn integration_test_ipfs_store_key() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let ks = s.list_keys().await?;
        assert!(ks
            .into_iter()
            .map(|k| k.name)
            .contains(&TEST_IPFS_KEYPAIR.to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_ipfs_store_ipns_resolve() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let b = randbytes(32);
        let res = s
            .add_item("testy", Bytes::copy_from_slice(&b))
            .await
            .unwrap();
        s.ipns_publish(&res.hash).await?;
        let cid = s.ipns_resolve(&s.key_id().await?).await?;
        assert_eq!(cid, res.hash);
        Ok(())
    }
    #[tokio::test]
    async fn integration_test_ipfs_store_objstore_get_put() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let b = randbytes(32);
        let path = s.ipns_as_obj_store_path().await?;
        s.put(&path, b.clone().into()).await?;
        let res = s.get(&path).await?;
        let b2 = res.bytes().await?;
        assert_eq!(b2.to_vec(), b);
        Ok(())
    }
    #[test_case(32; "32_byte_object")]
    #[test_case(1000; "kb_object")]
    #[test_case(1_000_000; "mb_object")]
    #[tokio::test]
    async fn integration_test_ipfs_store_ipns_get(n: usize) -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let b = randbytes(n);
        let res = s.add_item("testy", Bytes::copy_from_slice(&b)).await?;
        s.ipns_publish(&res.hash).await?;
        let data = s.ipns_get(&s.key_id().await?).await?;
        assert_eq!(data.to_vec(), b);
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_ipfs_store_add() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let n = 32;
        let randbytes = randbytes(n);
        let res = s
            .add_item("testy", Bytes::copy_from_slice(&randbytes))
            .await?;
        s.pin_item(&res.hash).await.unwrap();
        let res2 = s
            .add_item("testy22", Bytes::copy_from_slice(&randbytes))
            .await?;
        assert_eq!(res2.hash, res.hash);
        let res3 = s
            .add_item("testy", Bytes::copy_from_slice(&randbytes[..20]))
            .await?;
        assert_ne!(res3.hash, res.hash);
        Ok(())
    }
    #[test_case(32; "32_byte_object")]
    #[test_case(1000; "kb_object")]
    #[test_case(1_000_000; "mb_object")]
    #[test_case(0; "empty_object")]
    #[tokio::test]
    async fn integration_test_ipfs_store_get(n: usize) -> Result<()> {
        ipfs_integration_test!();
        let store = ipfs_store().await;
        let (randbytes, cid) = testybytes(n).await;
        let res = store.get(&store.path_to_obj_store(&cid)).await?;
        let dat = res.bytes().await?;
        assert_eq!(dat.to_vec(), randbytes);
        Ok(())
    }

    #[test_case(32,0,16 ; "first_half")]
    #[test_case(32,0,32 ; "full_range")]
    #[test_case(32,5,5 ; "empty_range")]
    #[test_case(32,5,0 ; "neg_range")]
    #[tokio::test]
    async fn integration_test_ipfs_store_get_range(
        objsize: usize,
        lower: usize,
        upper: usize,
    ) -> Result<()> {
        ipfs_integration_test!();
        let store = ipfs_store().await;
        let (randbytes, cid) = testybytes(objsize).await;
        let res = store
            .get_range(&store.path_to_obj_store(&cid), lower..upper)
            .await?;
        let expectlen = upper.saturating_sub(lower);
        assert_eq!(res.len(), expectlen);
        if expectlen > 0 {
            assert_eq!(res, &randbytes.as_slice()[lower..upper]);
        }
        Ok(())
    }

    #[test_case(32; "32_byte_object")]
    #[test_case(1_000_000; "mb_object")]
    #[test_case(0; "empty_object")]
    #[tokio::test]
    async fn integration_test_ipfs_store_head(n: usize) -> Result<()> {
        ipfs_integration_test!();
        let store = ipfs_store().await;
        let (randbytes, cid) = testybytes(n).await;
        let res = store.head(&store.path_to_obj_store(&cid)).await?;
        assert_eq!(res.size, randbytes.len());
        Ok(())
    }
    #[tokio::test]
    async fn integration_test_ipfs_store_get_cid_not_found() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let cid = "QmbWqxBEKC3XXXXXKc98xmWNzrzDtRLMiMPL8wBuTGsMnR";
        let res = s.get(&s.path_to_obj_store(cid)).await.unwrap_err();
        match res {
            Error::NotFound { path, .. } => {
                assert_eq!(path, format!("{cid}"));
            }
            _ => panic!("unexpected error variant!"),
        }
        Ok(())
    }
    #[tokio::test]
    async fn integration_test_ipfs_store_head_cid_not_found() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let cid = "QmbWqxBEKC3XXXXXKc98xmWNzrzDtRLMiMPL8wBuTGsMnR";
        let res = s.head(&s.path_to_obj_store(cid)).await.unwrap_err();
        match res {
            Error::NotFound { path, .. } => {
                assert_eq!(path, format!("{cid}"));
            }
            _ => panic!("unexpected error variant!"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn integration_test_ipfs_store_invalid_cid() -> Result<()> {
        ipfs_integration_test!();
        let s = ipfs_store().await;
        let cid = "wtfisthis";
        let res = s.head(&s.path_to_obj_store(cid)).await.unwrap_err();
        match res {
            Error::NotFound { .. } => {}
            // Error::NotFound { path, .. } => {
            //     assert_eq!(path, format!("/ipfs/{cid}"));
            // }
            _ => panic!("unexpected error variant!"),
        }
        Ok(())
    }
}
