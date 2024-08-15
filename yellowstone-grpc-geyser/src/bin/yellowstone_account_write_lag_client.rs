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

    tokio::spawn(grpc_client1());
    tokio::spawn(grpc_client2());

    loop {
        debug!("CLIENT STILL RUNNING");
        sleep(Duration::from_millis(5000));
    }
}

async fn grpc_client1() {

    let grpc_addr = std::env::var("GRPC_ADDR").expect("need grpc url");


    let mut client = GeyserClient::connect(grpc_addr).await.expect("connected");
    let mut stream = client.subscribe(once(async move { subscribe_processed_slots() })).await.expect("subscripbe").into_inner();


    loop {
        let response = stream.next().await.expect("response");
        match response.unwrap().update_oneof.unwrap() {
            UpdateOneof::Slot(update_slot) => {
                assert_eq!(update_slot.status, 0, "processed only - fix your subscription");
                info!("SLT slot {} ({})", update_slot.slot, update_slot.status);
            }
            UpdateOneof::Account(update_acount) => {
                info!("SLOT account write at {}", update_acount.slot);
                unreachable!("should not be here");
            }
            _ => {
                panic!("other");
            }
        }
    }

}




async fn grpc_client2() {

    let grpc_addr = std::env::var("GRPC_ADDR").expect("need grpc url");


    let mut client = GeyserClient::connect(grpc_addr).await.expect("connected");
    let mut stream = client.subscribe(once(async move { subscribe_orca() })).await.expect("subscripbe").into_inner();


    loop {
        let response = stream.next().await.expect("response");
        match response.unwrap().update_oneof.unwrap() {
            UpdateOneof::Slot(update_slot) => {
                assert_eq!(update_slot.status, 0, "processed only - fix your subscription");
                info!("ORCA slot {} ({})", update_slot.slot, update_slot.status);
                unreachable!("should not be here");
            }
            UpdateOneof::Account(update_acount) => {
                info!("ORCA account write at {}", update_acount.slot);
            }
            _ => {
                panic!("other");
            }
        }
    }

}




fn subscribe_processed_slots() -> SubscribeRequest {
    let mut slot_subs = HashMap::new();
    slot_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        // one processed
        filter_by_commitment: Some(true),
    });

    SubscribeRequest {
        slots: slot_subs,
        ping: None,
        // implies accounts at processed level
        commitment: Some(0), // TODO clarify
        ..Default::default()
    }
}


fn subscribe_orca() -> SubscribeRequest {
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
        accounts: accounts_subs,
        ping: None,
        // implies accounts at processed level
        commitment: Some(0), // TODO clarify
        ..Default::default()
    }
}




fn subscribe_combined_request() -> SubscribeRequest {
    let mut slot_subs = HashMap::new();
    slot_subs.insert("client".to_string(), SubscribeRequestFilterSlots {
        // one processed
        filter_by_commitment: Some(true),
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
        commitment: Some(0), // TODO clarify
        ..Default::default()
    }
}

