use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
    SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

pub use crate::{
    GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult,
};

pub type AtomicSlot = Arc<AtomicU64>;

// 1-based attempt counter
pub type Attempt = u32;

// wraps payload and status messages
// clone is required by broacast channel
#[derive(Clone)]
pub enum Message {
    GeyserSubscribeUpdate(Box<SubscribeUpdate>),
    // connect (attempt=1) or reconnect(attempt=2..)
    Connecting(Attempt),
}

#[derive(Clone, Debug)]
pub struct GrpcConnectionTimeouts {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub subscribe_timeout: Duration,
    pub receive_timeout: Duration,
}

#[derive(Clone)]
pub struct GrpcSourceConfig {
    pub grpc_addr: String,
    pub grpc_x_token: Option<String>,
    pub(crate) tls_config: Option<ClientTlsConfig>,
    pub(crate) timeouts: Option<GrpcConnectionTimeouts>,
}

impl Display for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grpc_addr {}",
            crate::obfuscate::url_obfuscate_api_token(&self.grpc_addr)
        )
    }
}

impl Debug for GrpcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl GrpcSourceConfig {
    /// Create a grpc source without tls and timeouts
    pub fn new_simple(grpc_addr: String) -> Self {
        Self {
            grpc_addr,
            grpc_x_token: None,
            tls_config: None,
            timeouts: None,
        }
    }
    pub fn new(
        grpc_addr: String,
        grpc_x_token: Option<String>,
        tls_config: Option<ClientTlsConfig>,
        timeouts: GrpcConnectionTimeouts,
    ) -> Self {
        Self {
            grpc_addr,
            grpc_x_token,
            tls_config,
            timeouts: Some(timeouts),
        }
    }
}

#[derive(Clone)]
pub struct GeyserFilter(pub CommitmentConfig);

impl GeyserFilter {
    pub fn blocks_and_txs(&self) -> SubscribeRequest {
        let mut blocks_subs = HashMap::new();
        blocks_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: Default::default(),
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        );

        SubscribeRequest {
            blocks: blocks_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn blocks_meta(&self) -> SubscribeRequest {
        let mut blocksmeta_subs = HashMap::new();
        blocksmeta_subs.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

        SubscribeRequest {
            blocks_meta: blocksmeta_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn slots(&self) -> SubscribeRequest {
        let mut slots_subs = HashMap::new();
        slots_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
            },
        );

        SubscribeRequest {
            slots: slots_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }

    pub fn accounts(&self) -> SubscribeRequest {
        let mut accounts_subs = HashMap::new();
        accounts_subs.insert(
            "client".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
            },
        );

        SubscribeRequest {
            accounts: accounts_subs,
            commitment: Some(map_commitment_level(self.0) as i32),
            ..Default::default()
        }
    }
}

pub fn map_commitment_level(commitment_config: CommitmentConfig) -> CommitmentLevel {
    // solana_sdk -> yellowstone
    match commitment_config.commitment {
        solana_sdk::commitment_config::CommitmentLevel::Processed => CommitmentLevel::Processed,
        solana_sdk::commitment_config::CommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
        solana_sdk::commitment_config::CommitmentLevel::Finalized => CommitmentLevel::Finalized,
    }
}
