/// raw data returned by the RPC endpoints we want to hit
use serde_derive::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub number: String,
    pub hash: String,
    pub base_fee_per_gas: Option<String>,
    pub difficulty: Option<String>,
    pub extra_data: Option<String>,
    pub gas_limit: Option<String>,
    pub gas_used: Option<String>,
    pub logs_bloom: Option<String>,
    pub miner: Option<String>,
    pub mix_hash: Option<String>,
    pub nonce: Option<String>,
    pub parent_hash: Option<String>,
    pub receipts_root: Option<String>,
    #[serde(rename = "sha3Uncles")]
    pub sha3_uncles: Option<String>,
    pub size: Option<String>,
    pub state_root: Option<String>,
    pub timestamp: Option<String>,
    pub total_difficulty: Option<String>,
    pub transactions_root: Option<String>,
    #[serde(default)]
    pub uncles: Vec<String>,
    #[serde(default)]
    pub transactions: Vec<Txn>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    pub block_hash: String,
    pub block_number: String,
    pub from: Option<String>,
    pub gas: Option<String>,
    pub gas_price: Option<String>,
    pub hash: String,
    pub input: Option<String>,
    pub nonce: Option<String>,
    pub to: Option<String>,
    pub transaction_index: Option<String>,
    pub value: Option<String>,
    #[serde(rename = "type")]
    pub tx_type: Option<String>,
    pub chain_id: Option<String>,
    pub v: Option<String>,
    pub r: Option<String>,
    pub s: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxnReceipt {
    pub block_hash: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub contract_address: Option<String>,
    pub cumulative_gas_used: Option<String>,
    pub effective_gas_price: Option<String>,
    pub from: Option<String>,
    pub gas_used: Option<String>,
    pub logs: Vec<Log>,
    pub logs_bloom: Option<String>,
    pub status: Option<String>,
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub receipt_type: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub block_number: String,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub log_index: String,
    pub address: Option<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    pub partition_data: Option<String>,
    pub removed: Option<bool>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub action: Option<TraceAction>,
    pub block_hash: String,
    pub block_number: i64,
    pub result: Option<TraceResult>,
    pub subtraces: i64,
    pub trace_address: Vec<i64>,
    pub transaction_hash: Option<String>,
    pub transaction_position: Option<i64>,
    #[serde(rename = "type")]
    pub trace_type: String,
    pub error: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceAction {
    pub from: Option<String>,
    pub call_type: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub init: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResult {
    pub gas_used: Option<String>,
    pub output: Option<String>,
    pub address: Option<String>,
    pub code: Option<String>,
}
