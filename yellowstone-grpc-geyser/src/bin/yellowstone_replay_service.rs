use std::io;
use std::io::BufRead;
use bytes::{Bytes, BytesMut};
use rand::distributions::Standard;
use rand::{random, thread_rng, Rng, RngCore};
use solana_geyser_plugin_interface::geyser_plugin_interface::{GeyserPluginError, SlotStatus};
use solana_sdk::clock::{Slot, UnixTimestamp};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::recent_blockhashes_account::update_account;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::thread::{sleep, spawn};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use itertools::Itertools;
use log::{debug, info, trace};
use solana_sdk::signer::SignerError::KeypairPubkeyMismatch;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep_until};
use yellowstone_grpc_geyser::config::{ConfigBlockFailAction, ConfigGrpc, ConfigGrpcFilters};
use yellowstone_grpc_geyser::grpc::{
    GrpcService, Message, MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageSlot,
};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

const RAYDIUM_AMM_PUBKEY: &'static str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("starting mock service");

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
        debug!("MOCK STILL RUNNING");
        sleep(Duration::from_millis(1000));
    }
}

// - 20-80 MiB per Slot
// 4000 updates per Slot
async fn mainnet_traffic(grpc_channel: UnboundedSender<Message>) {

    let mut slot_cnt = 0;
    let mut account_cnt = 0;
    let started_at = Instant::now();

    let buffer = {
        info!("use data from inline csv");
        // 500k
        let data = include_bytes!("dump-slot-acccounts-fsn4-mixed-500k.csv");
        io::BufReader::new(data.as_ref())
    };

    let owner = Pubkey::from_str(RAYDIUM_AMM_PUBKEY).unwrap();

    let mut last_message_timestamp: u64 = 0;
    let mut last_sent_at: Instant = Instant::now().sub(Duration::from_secs(100));

    for line in buffer.lines().flatten() {
        // update_slot.slot, update_slot.parent.unwrap_or(0), short_status, since_epoch_ms
        // slot, account_pk, write_version, data_len, since_epoch_ms
        let rows = line.split(",").collect_vec();

        let mut message_timestamp;

        if rows[0] == "MIXSLOT" {
            let slot: u64 = rows[1].parse().unwrap();
            let parent: Option<u64> = rows[2].parse().ok().and_then(|v| if v == 0 { None } else { Some(v) });
            let commitment_level = match rows[3].to_string().as_str() {
                "P" => CommitmentLevel::Processed,
                "C" => CommitmentLevel::Confirmed,
                "F" => CommitmentLevel::Finalized,
                _ => panic!("invalid commitment level"),
            };
            let since_epoch_ms: u64 = rows[4].trim().parse().unwrap();

            let slot_status = match commitment_level {
                CommitmentLevel::Processed => SlotStatus::Processed,
                CommitmentLevel::Confirmed => SlotStatus::Confirmed,
                CommitmentLevel::Finalized => SlotStatus::Rooted,
                _ => panic!("invalid commitment level"),
            };
            const INIT_CHAIN: Slot = 0;
            trace!("MIXSLOT slot: {}, parent: {:?}, status: {:?}, since_epoch_ms: {}", slot, parent, slot_status, since_epoch_ms);
            slot_cnt += 1;
            message_timestamp = since_epoch_ms;
            let slot_update = MessageSlot {
                slot,
                parent,
                status: commitment_level,
            };
            grpc_channel.send(Message::Slot(slot_update)).expect("channel was closed");

        } else if rows[0] == "MIXACCOUNT" {
            let slot: u64 = rows[1].parse().unwrap();
            let account_pk: String = rows[2].parse().unwrap();
            let account_pk = Pubkey::from_str(&account_pk).unwrap();
            let write_version: u64 = rows[3].parse().unwrap();
            let data_len: u64 = rows[4].parse().unwrap();
            let since_epoch_ms: u64 = rows[5].trim().parse().unwrap();
            trace!("MIXACCOUNT slot: {}, account_pk: {}, write_version: {}, data_len: {}, since_epoch_ms: {}", slot, account_pk, write_version, data_len, since_epoch_ms);

            account_cnt += 1;
            message_timestamp = since_epoch_ms;
            let account_update = MessageAccount {
                slot,
                account: MessageAccountInfo {
                    pubkey: account_pk,
                    lamports: 9999,
                    owner,
                    executable: false,
                    rent_epoch: 0,
                    data: vec![42u8; data_len as usize],
                    write_version,
                    txn_signature: None,
                },
                is_startup: false,
            };
            grpc_channel.send(Message::Account(account_update)).expect("channel was closed");
        } else {
            panic!("invalid row: {:?}", rows);
        }

        if last_message_timestamp == 0 {
            last_message_timestamp = message_timestamp;
        }

        let message_diff_ms = message_timestamp - last_message_timestamp;
        last_message_timestamp = message_timestamp;

        // trace!("need to wait {}ms", message_diff_ms);

        let wait_until = last_sent_at.add(Duration::from_millis(message_diff_ms));
        last_sent_at = Instant::now();

        if (slot_cnt + account_cnt) % 100_000 == 0 {
            info!("progress .. slot_cnt: {}, account_cnt: {}, elapsed: {:.02}s", slot_cnt, account_cnt, started_at.elapsed().as_secs_f64());
        }

        sleep_until(wait_until).await;
    }

    info!("slot_cnt: {}, account_cnt: {}, elapsed: {:.02}s", slot_cnt, account_cnt, started_at.elapsed().as_secs_f64());


}
