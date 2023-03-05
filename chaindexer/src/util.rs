//! Self-contained utils that are useful in various plaes.
use std::{io, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use num::{BigUint, Num};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// common config object for interacting with rpc apis
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RpcApiConfig {
    /// url to access the rpc on. e.g. `http://localhost:8545`
    pub url: Option<String>,
    /// max amount of rpc calls in a single batch request
    pub batch_size: Option<usize>,
    /// maximum amount of requests to the rpc api that can occur at a time.
    pub max_concurrent: Option<usize>,
    /// how long to wait before giving up and returning a timeout error.
    pub request_timeout_ms: Option<u64>,
    pub cache_dir: Option<PathBuf>,
}

impl Default for RpcApiConfig {
    fn default() -> Self {
        Self {
            // NB: defaults to no URL b/c this could be anything. all other fields will
            // work decently with the provided defaults.
            url: None,
            batch_size: Some(100),
            max_concurrent: Some(10),
            request_timeout_ms: Some(10_000),
            cache_dir: None,
        }
    }
}
/// simple buff that just stores data in memory. the buf can be flushed empty.
/// when flushed empty, the caller takes ownership of the flushed data.
#[derive(Clone)]
pub struct SwappableMemBuf {
    buf: Arc<parking_lot::Mutex<Vec<u8>>>,
}
impl SwappableMemBuf {
    pub fn new() -> Self {
        Self {
            buf: Arc::new(parking_lot::Mutex::new(vec![])),
        }
    }
    /// take ownership of underlying buff, wrap it in bytes and replace it with a new
    /// empty buf.
    pub fn flush_empty(&self) -> Bytes {
        let mut buf = self.buf.lock();
        let mut out: Vec<u8> = vec![];
        std::mem::swap(&mut out, &mut buf);
        Bytes::from(out)
    }

    pub fn is_empty(&self) -> bool {
        let buf = self.buf.lock();
        buf.is_empty()
    }
}
impl io::Write for SwappableMemBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut b = self.buf.lock();
        b.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        // no-op
        Ok(())
    }
}

pub fn logs_enabled() -> bool {
    std::env::var("RUST_LOG").is_ok()
}

/// Takes a json value `obj`, adds key value pair to it, and returns
pub fn add_key_val(obj: Value, key: &str, val: Value) -> Value {
    if let Value::Object(mut m) = obj {
        m.insert(key.to_string(), val);
        Value::from(m)
    } else {
        // no-op if its not an obj
        obj
    }
}

/// Decode hex strings that might start with hex prefix
pub fn decode_hex(s: &String) -> Result<Vec<u8>> {
    hex::decode(if hex_str_has_prefix(s) { &s[2..] } else { s })
        .map_err(|_| anyhow!("Hex decode failed for {s}"))
}

/// Does hex string `s` start with prefix
fn hex_str_has_prefix(s: &String) -> bool {
    if s.len() <= 2 {
        false
    } else {
        ["0x", "\\x"].contains(&&s[0..2])
    }
}

/// Get integer from input string interpreted as base16 digits
pub fn hex_to_int(s: &String) -> Result<u64> {
    let sliced = {
        if hex_str_has_prefix(s) {
            &s[2..]
        } else {
            s
        }
    };
    Ok(u64::from_str_radix(sliced, 16)?)
}

/// Like [hex_to_int] but returns a [num::BigUint]
pub fn hex_to_big_int(s: &String) -> Result<num::BigUint> {
    let sliced = {
        if hex_str_has_prefix(s) {
            &s[2..]
        } else {
            s
        }
    };
    Ok(BigUint::from_str_radix(sliced, 16)?)
}

/// convert `n` bytes into a human readable string.
/// e.g. `bytes_to_h(1024) = "1.0KB"
pub fn bytes_to_h(n: usize) -> String {
    let suffixes = vec!["B", "KB", "MB", "GB", "TB", "PB"];
    let mut nbytes = n as f64;
    let mut i = 0;
    while nbytes >= 1024.0 && i < suffixes.len() - 1 {
        nbytes /= 1024.0;
        i += 1;
    }
    let suffix = suffixes[i];
    format!("{nbytes:.2}{suffix}")
}

/// count the number of rows in a list of arrow [`RecordBatch`] objects
pub fn batch_count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().fold(0, |acc, cur| acc + cur.num_rows())
}

pub fn randbytes(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(rand::random());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::H256;
    use num::FromPrimitive;
    use serde_json::json;
    use std::io::Write;

    #[test]
    fn membuff_read_write() {
        let bytes = b"testy 123456".as_slice();
        let mut buff = SwappableMemBuf::new();
        buff.write_all(bytes).unwrap();
        let out = buff.flush_empty();
        assert!(buff.is_empty());
        assert_eq!(out, bytes);
        let bytes2 = b"asdfasdf testy 123456".as_slice();
        buff.write_all(bytes2).unwrap();
        let out = buff.flush_empty();
        assert!(buff.is_empty());
        assert_eq!(out, bytes2);
    }
    #[test]
    fn membuff_clone() {
        let bytes = b"testy 123456".as_slice();
        let mut buff = SwappableMemBuf::new();
        buff.write_all(bytes).unwrap();
        let buff2 = buff.clone();
        assert!(Arc::ptr_eq(&buff.buf, &buff2.buf));
    }
    #[test]
    fn test_hex_to_bigint() {
        let smallone = "0xf4240".to_string();
        let b1 = hex_to_big_int(&smallone).unwrap();
        assert_eq!(b1, BigUint::from_usize(1_000_000).unwrap());
        let bigone =
            "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string();
        let b2 = hex_to_big_int(&bigone).unwrap();
        assert_eq!(
            b2.to_str_radix(10),
            "7237005577332262213973186563042994240829374041602535252466099000494570602495"
                .to_string()
        );
    }

    #[test]
    fn test_add_key_val() {
        let obj = json!({"a": 1, "b": 2});
        let newvalue = json!(3);
        let newkey = "c";
        let newobj = add_key_val(obj, newkey, newvalue.to_owned());
        if let Value::Object(v) = newobj {
            assert_eq!(v.get(newkey).unwrap().to_owned(), newvalue);
        } else {
            panic!();
        }
    }
    #[test]
    fn test_add_key_val_noop() {
        let obj = json!(1);
        let newone = add_key_val(obj.to_owned(), "1", json!(2));
        assert_eq!(obj, newone);
    }
    #[test]
    fn test_decode_eth_hex() {
        let h = H256::random();
        let cases = vec![
            ["0x".to_string(), hex::encode(h)].join(""),
            ["\\x".to_string(), hex::encode(h)].join(""),
            hex::encode(h),
        ];
        for c in &cases {
            let res = decode_hex(c).unwrap();
            assert_eq!(res, h.as_bytes());
        }
    }

    #[test]
    fn test_hex_int() {
        let h = "0xf4240".to_string();
        let i = hex_to_int(&h).unwrap();
        assert_eq!(i, 1_000_000);
        let h = "0x7fffffffffffffff".to_string();
        let i = hex_to_int(&h).unwrap();
        assert_eq!(i, 9223372036854775807);
        let h = "0x00".to_string();
        let i = hex_to_int(&h).unwrap();
        assert_eq!(i, 0);
    }

    #[test]
    fn test_bytes_to_h() {
        let f = bytes_to_h(2_097_152);
        assert_eq!(f, "2.00MB");
        let f = bytes_to_h(1073741824);
        assert_eq!(f, "1.00GB");
        let f = bytes_to_h(1073741824 * 1024 * 1024);
        assert_eq!(f, "1.00PB");
    }
}
