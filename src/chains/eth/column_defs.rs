//! Export column definitions so they can be used by other chains.
use crate::chains::ColumnTypeDef::*;
use crate::{
    chains::{
        eth::{Block, Log},
        ColumnDef,
    },
    util::{decode_hex, hex_to_big_int, hex_to_int},
};
use num::ToPrimitive;

pub fn blocks<'a>() -> Vec<ColumnDef<'a, Block>> {
    vec![
        ColumnDef {
            name: "number",
            nullable: false,
            transform: U64 {
                from_raw: |x| hex_to_int(&x.number).ok(),
            },
        },
        ColumnDef {
            name: "hash",
            nullable: false,
            transform: FixedBytes {
                from_raw: |x| decode_hex(&x.hash).ok(),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "timestamp",
            nullable: true,
            transform: Timestamp {
                from_raw: |x| {
                    x.timestamp
                        .as_ref()
                        .and_then(|y| hex_to_int(y).map(|o| o as i64).ok())
                },
            },
        },
        ColumnDef {
            name: "base_fee_per_gas",
            nullable: true,
            transform: U64 {
                from_raw: |x| {
                    x.base_fee_per_gas.as_ref().and_then(|h| hex_to_int(h).ok())
                    // .flatten()
                },
            },
        },
        ColumnDef {
            name: "difficulty",
            nullable: true,
            transform: U64 {
                from_raw: |x| {
                    x.difficulty.as_ref().and_then(|h| hex_to_int(h).ok())
                    // .flatten()
                },
            },
        },
        ColumnDef {
            name: "total_difficulty",
            nullable: true,
            transform: Float64 {
                from_raw: |x| {
                    x.total_difficulty
                        .as_ref()
                        .and_then(|h| hex_to_big_int(h).ok())
                        .and_then(|b| b.to_f64())
                    // .flatten()
                },
            },
        },
        ColumnDef {
            name: "gas_limit",
            nullable: true,
            transform: Float64 {
                from_raw: |x| {
                    x.gas_limit
                        .as_ref()
                        .and_then(|h| hex_to_big_int(h).ok())
                        .and_then(|b| b.to_f64())
                },
            },
        },
        ColumnDef {
            name: "parent_hash",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| x.parent_hash.as_ref().and_then(|h| decode_hex(h).ok()),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "nonce",
            nullable: true,
            transform: Bytes {
                from_raw: |x| x.nonce.as_ref().and_then(|h| decode_hex(h).ok()),
            },
        },
        ColumnDef {
            name: "miner",
            nullable: true,
            transform: FixedBytes {
                num_bytes: 20,
                from_raw: |x| x.miner.as_ref().and_then(|h| decode_hex(h).ok()),
            },
        },
        ColumnDef {
            name: "size",
            nullable: true,
            transform: U64 {
                from_raw: |x| x.size.as_ref().and_then(|h| hex_to_int(h).ok()),
            },
        },
    ]
}
fn get_topic(log: &Log, idx: usize) -> Option<Vec<u8>> {
    let hextopic = log.topics.get(idx);
    hextopic.and_then(|ht| decode_hex(ht).ok())
}
pub fn logs<'a>() -> Vec<ColumnDef<'a, Log>> {
    vec![
        ColumnDef {
            name: "block_number",
            nullable: false,
            transform: U64 {
                from_raw: |x| hex_to_int(&x.block_number).ok(),
            },
        },
        ColumnDef {
            name: "block_hash",
            nullable: false,
            transform: FixedBytes {
                from_raw: |x| decode_hex(&x.block_hash).ok(),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "contract_address",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| x.address.as_ref().and_then(|y| decode_hex(y).ok()),
                num_bytes: 20,
            },
        },
        ColumnDef {
            name: "data",
            nullable: true,
            transform: Blob {
                from_raw: |x| x.address.as_ref().and_then(|y| decode_hex(y).ok()),
            },
        },
        ColumnDef {
            name: "index",
            nullable: false,
            transform: U64 {
                from_raw: |x| hex_to_int(&x.log_index).ok(),
            },
        },
        ColumnDef {
            name: "topic1",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| get_topic(x, 0),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "topic2",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| get_topic(x, 1),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "topic3",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| get_topic(x, 2),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "topic4",
            nullable: true,
            transform: FixedBytes {
                from_raw: |x| get_topic(x, 3),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "tx_hash",
            nullable: false,
            transform: FixedBytes {
                from_raw: |x| decode_hex(&x.transaction_hash).ok(),
                num_bytes: 32,
            },
        },
        ColumnDef {
            name: "tx_index",
            nullable: false,
            transform: U64 {
                from_raw: |x| hex_to_int(&x.transaction_index).ok(),
            },
        },
    ]
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;
    use itertools::Itertools;

    use super::*;
    #[test]
    fn test_get_topic_helper() {
        let mut log = Log {
            ..Default::default()
        };

        fn gentopics(l: usize) -> Vec<String> {
            (0..l)
                .map(|_| format!("0x{}", hex::encode(H256::random().as_bytes())))
                .collect_vec()
        }
        log.topics = gentopics(4);
        for idx in 0..4 {
            let t = get_topic(&log, idx);
            assert!(t.is_some());
        }
        log.topics = gentopics(3);
        assert!(get_topic(&log, 3).is_none());
        log.topics = vec![];
        for idx in 0..4 {
            let t = get_topic(&log, idx);
            assert!(t.is_none());
        }
    }
}
