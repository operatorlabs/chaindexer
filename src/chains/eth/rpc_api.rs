//! Code for fetching data with an Ethereum RPC node.

use super::raw_data;
use crate::table_api::BlockNumSet;
use crate::util::hex_to_int;
use crate::util::{add_key_val, RpcApiConfig};
use anyhow::{anyhow, Result};
use futures::future::try_join_all;
use itertools::Itertools;
use log::{debug, error};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct RpcApi {
    http_client: Client,
    /// identify the chain that this rpc api is for
    chain_id: String,
    url: String,
    batch_size: usize,
    sem: Semaphore,
    // req_cache: DashMap,
}

/// default values for an ethereum rpc
pub fn rpc_defaults() -> RpcApiConfig {
    RpcApiConfig {
        url: Some("http://localhost:8545".to_string()),
        ..Default::default()
    }
}

// #[derive(Debug, Clone)]
// pub struct RpcOpts {
//     pub url: String,
//     pub batch_size: usize,
//     pub max_concurrent: Option<usize>,
// }
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RpcErr {
    pub code: i16,
    pub message: String,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RpcRes {
    pub id: i16,
    pub error: Option<RpcErr>,
    pub result: Option<Value>,
    pub jsonrpc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RpcReq {
    pub method: String,
    pub params: Vec<Value>,
}

pub struct BatchRequestsFailed {
    pub errs: Vec<RpcErr>,
}

#[derive(Debug, Error)]
pub enum RpcApiError {
    #[error("rpc requests failed")]
    BatchRequestsFailed { errs: Vec<String> },

    #[error("non-200 http response")]
    HttpUnexpectedResponseCode { res: reqwest::Response },

    #[error("http request failed to send")]
    HttpRequestFailed { err: reqwest::Error },
    #[error("RPC api request timed out... check connection or try lowering batch size")]
    RequestTimeout,

    #[error("failed to deserialize RPC result")]
    PayloadDeserializationFailed { err: serde_json::Error },

    #[error("Received invalid user input")]
    InvalidInput { message: String },
}

impl RpcApi {
    pub fn new(chain_id: &str, conf: &RpcApiConfig) -> Self {
        let defaults = RpcApiConfig::default();
        macro_rules! get_or_default {
            ($i: ident) => {
                conf.$i.clone().unwrap_or_else(|| {
                    let v = defaults.$i.unwrap();
                    log::debug!(
                        "ethereum rpc api is using default value {v} for config var: {}",
                        stringify!($i)
                    );
                    v
                })
            };
        }

        let http_client = Client::builder()
            .timeout(Duration::from_millis(get_or_default!(request_timeout_ms)))
            .build()
            .unwrap();
        Self {
            http_client,
            chain_id: chain_id.to_owned(),
            url: get_or_default!(url),
            batch_size: get_or_default!(batch_size),
            sem: Semaphore::new(get_or_default!(max_concurrent)),
        }
    }

    pub async fn batch_rpc_req<T: DeserializeOwned + Sync + Send>(
        &self,
        calls: &[RpcReq],
    ) -> Result<Vec<T>> {
        // validate input: must be non empty and all methods must be the same for deserialization to work
        if calls.is_empty() {
            return Err(anyhow!(RpcApiError::InvalidInput {
                message: "received zero RPC requests".to_string(),
            }));
        }
        if calls.iter().any(|c| c.method != calls[0].method) {
            return Err(anyhow!(RpcApiError::InvalidInput {
                message: "received multiple different methods in batch RPC request".to_string(),
            }));
        }
        let method_name = calls.get(0).unwrap().method.to_owned();
        let _perm = self.sem.acquire().await?;
        debug!(
            "requesting batch of size {}, method={}",
            calls.len(),
            calls[0].method
        );
        let calls = calls
            .iter()
            .enumerate()
            .map(|(idx, call)| add_key_val(serde_json::to_value(call).unwrap(), "id", json!(idx)))
            .map(|call| add_key_val(call, "jsonrpc", json!("2.0")))
            .collect_vec();
        let req = self
            .http_client
            .request(Method::POST, &self.url)
            .header("content-type", "application/json")
            .json(&calls[..]);
        let res = req.send().await.map_err(|err| {
            anyhow!(match &err.is_timeout() {
                true => RpcApiError::RequestTimeout,
                false => RpcApiError::HttpRequestFailed { err },
            })
            .context(format!("HTTP request failed to complete for {method_name}",))
        })?;

        if res.status().as_u16() != 200 {
            let statuscode = res.status().as_u16();
            // TODO: add retry logic here
            error!("got http response code {}", statuscode);
            return Err(
                anyhow!(RpcApiError::HttpUnexpectedResponseCode { res }).context(format!(
                    "got response code of {statuscode} from method {method_name} ",
                )),
            );
        }
        // batch eth response returns array of individual response objects
        // each of which could be an error or a success
        let rpc_res = res.json::<Vec<RpcRes>>().await.map_err(|e| {
            error!("Failed to convert response into vector of RpcRes objects due to {e}");
            anyhow!(e).context(format!(
                "response deserialization failed for method {method_name}",
            ))
        })?;
        let (succ, fail): (Vec<RpcRes>, Vec<RpcRes>) = rpc_res
            .into_iter()
            .partition(|d| d.error.is_none() && d.result.is_some());
        if !fail.is_empty() {
            // TODO: add retry logic here
            let errs = fail
                .into_iter()
                .map(|f| f.error.unwrap().message)
                .collect_vec();
            let err_count = errs.len();
            return Err(
                anyhow!(RpcApiError::BatchRequestsFailed { errs }).context(format!(
                    "{err_count} requests within batch request failed for method {method_name}",
                )),
            );
        }
        let result: Vec<T> =
            succ.into_iter()
                .map(|d| {
                    serde_json::from_value::<T>(d.result.expect(
                        "unreachable code: already filtered out payloads where result=null",
                    ))
                })
                .try_collect()
                .map_err(|err| {
                    anyhow!(RpcApiError::PayloadDeserializationFailed { err }).context(format!(
                        "payload deserialization failed for method {method_name}"
                    ))
                })?;
        Ok(result)
    }

    /// Fetch all all blocks for numbers `blocknums`
    pub async fn blocks_with_nums(
        &self,
        nums: &'_ BlockNumSet<'_>,
    ) -> Result<Vec<raw_data::Block>> {
        let rpc_calls = nums
            .iter()
            .map(|num| RpcReq {
                method: "eth_getBlockByNumber".to_string(),
                params: vec![Value::from(format!("{num:#x}")), Value::Bool(true)],
            })
            .collect_vec();
        let futs = rpc_calls
            .chunks(self.batch_size)
            .map(|b| self.batch_rpc_req::<raw_data::Block>(b));
        Ok(try_join_all(futs).await?.into_iter().flatten().collect())
    }
    /// Get the transaction receipt objects between blocks start (inclusive)
    /// and end (exclusive).
    pub async fn tx_receipts_for_blocknums(
        &self,
        blocknums: &'_ BlockNumSet<'_>,
    ) -> Result<Vec<raw_data::TxnReceipt>> {
        let rpc_calls = blocknums
            .iter()
            .map(|num| RpcReq {
                method: "eth_getBlockReceipts".to_string(),
                params: vec![Value::from(format!("{num:#x}"))],
            })
            .collect_vec();
        let futs = rpc_calls
            .chunks(self.batch_size)
            .map(|b| self.batch_rpc_req::<Vec<raw_data::TxnReceipt>>(b));
        let results = try_join_all(futs)
            .await?
            .into_iter()
            .flatten()
            .flatten()
            .collect_vec();
        Ok(results)
    }

    pub async fn block_number(&self) -> Result<u64> {
        let req = vec![RpcReq {
            method: "eth_blockNumber".to_string(),
            params: vec![],
        }];
        let res = self.batch_rpc_req::<String>(&req[..]).await?;
        hex_to_int(&res[0])
    }
    /// Get all transaction receipts with the given `tx_hashes`.
    pub async fn tx_receipts_by_hash(
        &self,
        tx_hashes: Vec<String>,
    ) -> Result<Vec<raw_data::TxnReceipt>> {
        let rpc_calls = tx_hashes
            .into_iter()
            .map(|h| RpcReq {
                method: "eth_getTransactionReceipt".to_string(),
                params: vec![Value::from(h)],
            })
            .collect_vec();
        let futs = rpc_calls
            .chunks(self.batch_size)
            .map(|b| self.batch_rpc_req::<raw_data::TxnReceipt>(b));
        Ok(try_join_all(futs).await?.into_iter().flatten().collect())
    }

    pub async fn traces_in_range(&self, start: usize, end: usize) -> Result<Vec<raw_data::Trace>> {
        let rpc_calls = (start..end)
            .map(|num| RpcReq {
                method: "trace_block".to_string(),
                params: vec![Value::from(format!("{num:#x}"))],
            })
            .collect_vec();
        let futs = rpc_calls
            .chunks(self.batch_size)
            .map(|b| self.batch_rpc_req::<Vec<raw_data::Trace>>(b));
        let results = try_join_all(futs)
            .await?
            .into_iter()
            .flatten()
            .flatten()
            .collect_vec();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::{data_for_method, getrpc, mock_serv, start_block};
    use super::*;
    use crate::test::integration_test_flag;
    use log::info;

    #[tokio::test]
    async fn test_blocks_by_number_and_batching() -> Result<()> {
        // how many total requests are in the fixture for this method
        let req_count = 50;
        let method = "eth_getBlockByNumber";
        assert_eq!(data_for_method(method).request.len(), req_count);
        // test a few batch sizes
        for batch_size in [10, 12, 15, 50, 1] {
            let sblock = 1_000_000;
            let (u, mut bodys_sent) = tokio::time::timeout(
                tokio::time::Duration::from_millis(500),
                mock_serv(batch_size),
            )
            .await?;
            tokio::spawn(async move {
                let mut bodies: Vec<Vec<RpcReq>> = vec![];
                while let Some(b) = bodys_sent.recv().await {
                    bodies.push(b);
                }
                assert_eq!(bodies.len(), req_count / batch_size);
                for body in bodies {
                    assert_eq!(body.len(), batch_size);
                }
            });
            let rpc = RpcApi::new("eth", &conf(u, batch_size));
            let d = data_for_method(method);
            let a = rpc
                .blocks_with_nums(&BlockNumSet::Range(sblock, sblock + d.request.len() as u64))
                .await
                .unwrap();
            assert!(a.into_iter().map(|x| x.number).all_unique());
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_get_tx_receipts_by_hash() -> Result<()> {
        // just test serialzation stuff
        let method = "eth_getTransactionReceipt";
        let testdata = data_for_method(method);
        let batch_size = 10;
        let (u, _recv) = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            mock_serv(batch_size),
        )
        .await?;
        let rpc = RpcApi::new("eth", &conf(u, batch_size));
        let hashes: Vec<String> = testdata
            .request
            .iter()
            .map(|r| match r.params[0] {
                Value::String(ref s) => s.to_owned(),
                _ => {
                    panic!("invalid params in test data");
                }
            })
            .collect();
        rpc.tx_receipts_by_hash(hashes).await?;
        Ok(())
    }

    fn conf(url: String, batch_size: usize) -> RpcApiConfig {
        RpcApiConfig {
            url: Some(url),
            batch_size: Some(batch_size),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_get_tx_receipts_in_range() -> Result<()> {
        // just test serialzation stuff
        let method = "eth_getBlockReceipts";
        let testdata = data_for_method(method);
        let batch_size = 10;
        let (u, _recv) = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            mock_serv(batch_size),
        )
        .await?;
        let rpc = RpcApi::new("eth", &conf(u, batch_size));
        rpc.tx_receipts_for_blocknums(&BlockNumSet::Range(
            1_000_000,
            1_000_000 + testdata.request.len() as u64,
        ))
        .await?;
        Ok(())
    }
    #[tokio::test]
    async fn test_trace_blocks_range() -> Result<()> {
        // just test serialzation stuff
        let method = "trace_block";
        let testdata = data_for_method(method);
        let batch_size = 10;
        let (u, _recv) = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            mock_serv(batch_size),
        )
        .await?;
        let rpc = RpcApi::new("eth", &conf(u, batch_size));
        let traces = rpc
            .traces_in_range(1_000_000, 1_000_000 + testdata.request.len())
            .await?;
        let count = traces.iter().map(|t| t.block_number).unique().count();
        assert_eq!(count, testdata.request.len());
        Ok(())
    }

    // =============== integration tests =================
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
    #[macro_export]
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
    async fn integration_eth_rpc_blocks_by_number() -> Result<()> {
        maybe_check_rpc_exists_env!();
        let rpc = getrpc(10, 10);
        let sblock = start_block();
        let res = rpc.blocks_with_nums(&(sblock..sblock + 100).into()).await?;
        assert_eq!(res.len(), 100);
        Ok(())
    }
    #[tokio::test]
    async fn integration_eth_rpc_get_tx_receipts() -> Result<()> {
        maybe_check_rpc_exists_env!();
        let rpc = getrpc(10, 10);
        let block = start_block();
        // get transaction hashes
        let blocks_res = rpc.blocks_with_nums(&(block..block + 5).into()).await?;
        for rawblock in blocks_res {
            info!(
                "testing with block that has {} txs",
                rawblock.transactions.len()
            );
            let recpts = rpc
                .tx_receipts_by_hash(
                    rawblock
                        .transactions
                        .iter()
                        .map(|t| t.hash.to_owned())
                        .collect(),
                )
                .await?;
            assert_eq!(recpts.len(), rawblock.transactions.len());
        }
        Ok(())
    }
    #[tokio::test]
    async fn integration_eth_rpc_get_tx_receipts_in_range() -> Result<()> {
        maybe_check_rpc_exists_env!();
        let rpc = getrpc(100, 20);
        let sblock = start_block();
        let recpts = rpc
            .tx_receipts_for_blocknums(&BlockNumSet::Range(sblock, sblock + 50))
            .await?;
        let blocknums = recpts
            .iter()
            .map(|r| &r.block_number)
            .unique()
            .collect_vec();
        assert_eq!(blocknums.len(), 20);
        Ok(())
    }

    #[tokio::test]
    async fn integration_eth_rpc_trace_blocks() -> Result<()> {
        maybe_check_rpc_exists_env!();
        let rpc = getrpc(10, 20);
        let start_block = 15_000_000;
        let size = 100;
        // test several ranges
        for i in 0..5 {
            let block = start_block + i * size;
            let res = rpc.traces_in_range(block, block + size).await?;
            info!(
                "got {} traces in blocks {} to {}",
                res.len(),
                block,
                block + size
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn integration_eth_rpc_err() -> Result<()> {
        maybe_check_rpc_exists_env!();
        #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Testy {
            _testy: i64,
        }
        let sblock = start_block();
        let num_reqs = 10;
        let rpc_calls = (sblock..sblock + num_reqs)
            .map(|num| RpcReq {
                method: "eth_getBlockReceipts".to_string(),
                params: vec![Value::from(format!("{num:#x}")), Value::Bool(true)],
            })
            .collect_vec();
        let rpc = getrpc(100, 20);
        // placeholder struct, should error out before attempting to cast to struct
        // since we convert to RpcRes before converting to specific T
        let res = rpc.batch_rpc_req::<Testy>(&rpc_calls).await;
        assert!(res.is_err());
        if let Some(err) = res.unwrap_err().downcast_ref::<RpcApiError>() {
            match err {
                RpcApiError::BatchRequestsFailed { errs } => {
                    assert_eq!(errs.len() as u64, num_reqs);
                }
                _ => panic!(),
            }
        } else {
            panic!("failed to downcast error!");
        }
        Ok(())
    }
}
