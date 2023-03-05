use crate::util::RpcApiConfig;

use super::rpc_api::{RpcApi, RpcReq, RpcRes};
use once_cell::sync::Lazy;
use reqwest::get;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fs;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{self, Duration};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RpcTestData {
    pub request: Vec<RpcReq>,
    pub response: Vec<RpcRes>,
}
pub static TEST_DATA: Lazy<Vec<RpcTestData>> = Lazy::new(|| {
    let p = format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "testdata/eth-req-res-batch-tests.json"
    );
    let file = fs::File::open(p).expect("could not open test data!");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
});

pub fn data_for_method(method: &str) -> &RpcTestData {
    TEST_DATA
        .iter()
        .find(|x| x.request[0].method == method)
        .unwrap()
}

pub enum EthTable {
    Blocks,
    Logs,
    #[allow(unused)]
    Traces,
    #[allow(unused)]
    Transactions,
}
pub fn data_for_table(t: EthTable) -> &'static RpcTestData {
    use EthTable::*;
    data_for_method(match t {
        Blocks => "eth_getBlockByNumber",
        Logs => "eth_getBlockReceipts",
        Traces => "eth_getBlockByNumber",
        Transactions => "trace_block",
    })
}

async fn random_open_port() -> u16 {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.local_addr().unwrap().port()
}
#[derive(Clone)]
pub struct MockServerState {
    /// maintains a counter between requests to know chunk of test data to return
    counter: Arc<Mutex<usize>>,
    batch_size: usize,
    /// let tests see what request bodies are passed in
    send_body: Sender<Vec<RpcReq>>,
}
/// spin up a mock server on a random port and return sequential raw data batches
/// from the method specified
///
/// **NOTE: even if you arent going to use the receiver channel returned by this to monitor
/// paylaods sent, you need it in scope, otherwise this will panic**
pub async fn mock_serv(batch_size: usize) -> (String, Receiver<Vec<RpcReq>>) {
    let (send_body, recv_body) = tokio::sync::mpsc::channel::<Vec<RpcReq>>(1000);
    let state = MockServerState {
        counter: Arc::new(Mutex::new(0)),
        batch_size,
        send_body,
    };
    let mut app = tide::with_state(state);
    app.at("/")
        .post(|mut req: tide::Request<MockServerState>| async move {
            // find test data that matches method
            let reqbody: Vec<RpcReq> = req.body_json().await.unwrap();
            let meth = &reqbody[0].method.clone();
            let batchsize = &req.state().batch_size;
            let sendbody = &req.state().send_body;
            sendbody.send(reqbody).await?;
            // sendbody.send(reqbody).await.unwrap();
            let count = Arc::clone(&req.state().counter);
            let mut count = count.lock().unwrap();
            let idx = *count * batchsize;
            *count += 1;
            let d = &data_for_method(meth).to_owned();
            let upperidx = min(d.response.len(), idx + batchsize);
            // slice based on which request number this is
            let to_ret = &d.response[idx..upperidx];
            tide::Body::from_json(&to_ret)
        });
    app.at("/ping")
        .get(|_: tide::Request<MockServerState>| async {
            Ok(tide::Body::from_string("pong".to_string()))
        });
    let port = random_open_port().await;
    let url = format!("127.0.0.1:{port}");
    tokio::spawn(app.listen(url.clone()));
    // ping server til it responds
    loop {
        match get(format!("http://{url}/ping")).await {
            Err(_) => {
                time::sleep(Duration::from_millis(1)).await;
            }
            Ok(_) => {
                return (format!("http://{url}"), recv_body);
            }
        }
    }
}

pub fn getrpc(batch_size: usize, maxconc: usize) -> RpcApi {
    RpcApi::new(
        "eth",
        &RpcApiConfig {
            url: Some(get_rpc_url()),
            batch_size: Some(batch_size),
            max_concurrent: Some(maxconc),
            ..Default::default()
        },
    )
}
/// overridable value that defines the block number to start tests on...
/// default is just an arbitrary but fairly recent block (for eth mainnet)
pub fn start_block() -> u64 {
    let e = std::env::var("TEST_ETH_START_BLOCK");
    match e {
        Err(_) => 15_000_000,
        Ok(s) => s.parse::<u64>().unwrap(),
    }
}
/// Panics if env var TEST_ETH_RPC_URL not found.
pub fn get_rpc_url() -> String {
    std::env::var("TEST_ETH_RPC_URL").unwrap()
}
