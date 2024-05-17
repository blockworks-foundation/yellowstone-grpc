use bytes::Bytes;
use rand::distributions::Standard;
use rand::{random, thread_rng, Rng, RngCore};
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_sdk::clock::UnixTimestamp;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::recent_blockhashes_account::update_account;
use std::ops::Add;
use std::thread::{sleep, spawn};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use yellowstone_grpc_geyser::config::{ConfigBlockFailAction, ConfigGrpc, ConfigGrpcFilters};
use yellowstone_grpc_geyser::grpc::{
    GrpcService, Message, MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageSlot,
};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

#[tokio::main]
async fn main() {
    let config_grpc = ConfigGrpc {
        address: "127.0.0.1:50001".parse().unwrap(),
        tls_config: None,
        max_decoding_message_size: 4_000_000,
        snapshot_plugin_channel_capacity: None,
        snapshot_client_channel_capacity: 50_000_000,
        channel_capacity: 100_000,
        unary_concurrency_limit: 20,
        unary_disabled: false,
        filters: ConfigGrpcFilters::default(),
    };

    let (_snapshot_channel, grpc_channel, _grpc_shutdown) =
        GrpcService::create(config_grpc, ConfigBlockFailAction::Panic)
            .await
            .unwrap();

    tokio::spawn(mainnet_traffic(grpc_channel));

    loop {
        println!("MOCK STILL RUNNING");
        sleep(Duration::from_millis(1000));
    }
}

// - 20-80 MiB per Slot
// 4000 updates per Slot
async fn mainnet_traffic(grpc_channel: UnboundedSender<Message>) {
    let owner = Pubkey::new_unique();
    let account_pubkeys: Vec<Pubkey> = (0..100).map(|_| Pubkey::new_unique()).collect();

    for slot in 42_000_000.. {
        let slot_started_at = Instant::now();

        let sizes = vec![
            0, 8, 8, 165, 165, 165, 165, 11099, 11099, 11099, 11099, 11099, 11099,
        ];
        const target_bytes_total: usize = 30_000_000;
        let mut bytes_total = 0;

        let mut requested_sizes: Vec<usize> = Vec::new();

        for i in 0..99_999_999 {
            let data_size = sizes[i % sizes.len()];

            if bytes_total + data_size > target_bytes_total {
                break;
            }

            requested_sizes.push(data_size);
            bytes_total += data_size;
        }

        println!(
            "will send account updates down the stream ({} bytes) in {} messages",
            bytes_total,
            requested_sizes.len()
        );

        let avg_delay = 0.350 / requested_sizes.len() as f64;

        for (i, data_bytes) in requested_sizes.into_iter().enumerate() {
            let next_message_at =
                slot_started_at.add(Duration::from_secs_f64(avg_delay * i as f64));

            let data: Vec<u8> = thread_rng()
                .sample_iter(&Standard)
                .take(data_bytes)
                .collect();
            // using random slows down everything - could be the generator PRNG or the entropy preventing compression
            // let data: Vec<u8> = thread_rng().sample_iter(&Standard).take(data_bytes).collect();

            let account_pubkey = account_pubkeys[i % sizes.len()];

            let update_account = MessageAccount {
                account: MessageAccountInfo {
                    pubkey: account_pubkey,
                    lamports: 0,
                    owner,
                    executable: false,
                    rent_epoch: 0,
                    data,
                    write_version: 4321,
                    txn_signature: None,
                },
                slot,
                is_startup: false,
            };

            grpc_channel
                .send(Message::Account(update_account))
                .expect("channel was closed");

            tokio::time::sleep_until(next_message_at).await;
        }

        let block_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as UnixTimestamp;

        grpc_channel
            .send(Message::Slot(MessageSlot {
                slot,
                parent: Some(slot - 1),
                status: CommitmentLevel::Processed,
            }))
            .expect("channel was closed");

        grpc_channel
            .send(Message::BlockMeta(MessageBlockMeta {
                parent_slot: slot - 1,
                slot,
                parent_blockhash: "nohash".to_string(),
                blockhash: "nohash".to_string(),
                rewards: vec![],
                block_time: Some(block_time),
                block_height: None,
                executed_transaction_count: 0,
                entries_count: 0,
            }))
            .expect("channel was closed");

        tokio::time::sleep_until(slot_started_at.add(Duration::from_millis(400))).await;
    }
}

async fn helloworld_traffic(grpc_channel: UnboundedSender<Message>) {
    loop {
        let update_account = MessageAccount {
            account: MessageAccountInfo {
                pubkey: Default::default(),
                lamports: 0,
                owner: Default::default(),
                executable: false,
                rent_epoch: 0,
                data: vec![1, 2, 3],
                write_version: 0,
                txn_signature: None,
            },
            slot: 999_999,
            is_startup: false,
        };

        grpc_channel
            .send(Message::Account(update_account))
            .expect("send");
        println!("sent account update down the stream");

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
