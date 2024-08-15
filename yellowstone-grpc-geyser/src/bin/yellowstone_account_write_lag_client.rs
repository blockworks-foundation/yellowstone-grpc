use std::collections::HashMap;
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
use futures::stream::once;
use futures::StreamExt;
use itertools::Itertools;
use log::{debug, info, trace};
use solana_sdk::signer::SignerError::KeypairPubkeyMismatch;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep_until};
use yellowstone_grpc_geyser::config::{ConfigBlockFailAction, ConfigGrpc, ConfigGrpcFilters};
use yellowstone_grpc_geyser::grpc::{
    GrpcService, Message, MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageSlot,
};
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

const RAYDIUM_AMM_PUBKEY: &'static str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    grpc_client().await;

}


async fn grpc_client() {

    let grpc_addr = std::env::var("GRPC_ADDR").expect("need grpc url");


    let mut client = GeyserClient::connect(grpc_addr).await.expect("connected");
    let mut stream = client.subscribe(once(async move { subscribe_request() })).await.expect("subscripbe").into_inner();

    loop {
        let response = stream.next().await.expect("response");
        match response.unwrap().update_oneof.unwrap() {
            UpdateOneof::Slot(update_slot) => {
                if update_slot.status == 0 {
                    info!("slot {} ({})", update_slot.slot, update_slot.status);
                }
            }
            UpdateOneof::Account(update_acount) => {
                info!("account write at {}", update_acount.slot);
            }
            _ => {
                panic!("other");
            }
        }
    }

}


fn subscribe_request() -> SubscribeRequest {
    let mut slot_subs = HashMap::new();
    slot_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        // implies all slots
        filter_by_commitment: None,
    });
    let mut accounts_subs = HashMap::new();
    accounts_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec!["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string()], // orca
            filters: vec![],
        },
    );

    SubscribeRequest {
        slots: slot_subs,
        accounts: accounts_subs,
        ping: None,
        // implies accounts at processed level
        commitment: None,
        ..Default::default()
    }
}

