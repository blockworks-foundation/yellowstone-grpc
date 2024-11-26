use {
    backoff::{future::retry, ExponentialBackoff},
    clap::{Parser, Subcommand, ValueEnum},
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    log::{error, info},
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::TransactionError},
    solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionEncoding},
    std::{collections::{BTreeMap, HashMap}, env, fmt::{self, Display}, fs::File, sync::{atomic::AtomicU64, Arc}, time::{Duration, SystemTime}},
    tokio::{sync::Mutex, time::Instant},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, Interceptor},
    yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateAccount,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionStatus,
    },
};

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type TransactionsStatusFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type EntryFilterMap = HashMap<String, SubscribeRequestFilterEntry>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,

    #[command(subcommand)]
    action: Action,
}

const GPRC_CLIENT_BUFFER_SIZE: usize = 65536; // default: 1024
const GRPC_CONN_WINDOW: u32 = 5242880; // 5MB
const GRPC_STREAM_WINDOW: u32 = 4194304; // default: 2MB

impl Args {
    fn get_commitment(&self) -> Option<CommitmentLevel> {
        Some(self.commitment.unwrap_or_default().into())
    }

    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(self.x_token.clone())?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .buffer_size(GPRC_CLIENT_BUFFER_SIZE)
            .initial_connection_window_size(GRPC_CONN_WINDOW)
            .initial_stream_window_size(GRPC_STREAM_WINDOW)
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Subcommand)]
enum Action {
    HealthCheck,
    HealthWatch,
    Subscribe(Box<ActionSubscribe>),
    Ping {
        #[clap(long, short, default_value_t = 0)]
        count: i32,
    },
    GetLatestBlockhash,
    GetBlockHeight,
    GetSlot,
    IsBlockhashValid {
        #[clap(long, short)]
        blockhash: String,
    },
    GetVersion,
}

#[derive(Debug, Clone, clap::Args)]
struct ActionSubscribe {
    /// Subscribe on accounts updates
    #[clap(long)]
    accounts: bool,

    /// Filter by Account Pubkey
    #[clap(long)]
    accounts_account: Vec<String>,

    /// Path to a JSON array of account addresses
    #[clap(long)]
    accounts_account_path: Option<String>,

    /// Filter by Owner Pubkey
    #[clap(long)]
    accounts_owner: Vec<String>,

    /// Filter by Offset and Data, format: `offset,data in base58`
    #[clap(long)]
    accounts_memcmp: Vec<String>,

    /// Filter by Data size
    #[clap(long)]
    accounts_datasize: Option<u64>,

    /// Filter valid token accounts
    #[clap(long)]
    accounts_token_account_state: bool,

    /// Receive only part of updated data account, format: `offset,size`
    #[clap(long)]
    accounts_data_slice: Vec<String>,

    /// Subscribe on slots updates
    #[clap(long)]
    slots: bool,

    /// Filter slots by commitment
    #[clap(long)]
    slots_filter_by_commitment: bool,

    /// Subscribe on transactions updates
    #[clap(long)]
    transactions: bool,

    /// Filter vote transactions
    #[clap(long)]
    transactions_vote: Option<bool>,

    /// Filter failed transactions
    #[clap(long)]
    transactions_failed: Option<bool>,

    /// Filter by transaction signature
    #[clap(long)]
    transactions_signature: Option<String>,

    /// Filter included account in transactions
    #[clap(long)]
    transactions_account_include: Vec<String>,

    /// Filter excluded account in transactions
    #[clap(long)]
    transactions_account_exclude: Vec<String>,

    /// Filter required account in transactions
    #[clap(long)]
    transactions_account_required: Vec<String>,

    /// Subscribe on transactions_status updates
    #[clap(long)]
    transactions_status: bool,

    /// Filter vote transactions for transactions_status
    #[clap(long)]
    transactions_status_vote: Option<bool>,

    /// Filter failed transactions for transactions_status
    #[clap(long)]
    transactions_status_failed: Option<bool>,

    /// Filter by transaction signature for transactions_status
    #[clap(long)]
    transactions_status_signature: Option<String>,

    /// Filter included account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_include: Vec<String>,

    /// Filter excluded account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_exclude: Vec<String>,

    /// Filter required account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_required: Vec<String>,

    #[clap(long)]
    entry: bool,

    /// Subscribe on block updates
    #[clap(long)]
    blocks: bool,

    /// Filter included account in transactions
    #[clap(long)]
    blocks_account_include: Vec<String>,

    /// Include transactions to block message
    #[clap(long)]
    blocks_include_transactions: Option<bool>,

    /// Include accounts to block message
    #[clap(long)]
    blocks_include_accounts: Option<bool>,

    /// Include entries to block message
    #[clap(long)]
    blocks_include_entries: Option<bool>,

    /// Subscribe on block meta updates (without transactions)
    #[clap(long)]
    blocks_meta: bool,

    /// Send ping in subscribe request
    #[clap(long)]
    ping: Option<i32>,

    // Resubscribe (only to slots) after
    #[clap(long)]
    resub: Option<usize>,
}

impl Action {
    async fn get_subscribe_request(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> anyhow::Result<Option<(SubscribeRequest, usize)>> {
        Ok(match self {
            Self::Subscribe(args) => {
                let mut accounts: AccountFilterMap = HashMap::new();
                if args.accounts {
                    let mut accounts_account = args.accounts_account.clone();
                    if let Some(path) = args.accounts_account_path.clone() {
                        let accounts = tokio::task::block_in_place(move || {
                            let file = File::open(path)?;
                            Ok::<Vec<String>, anyhow::Error>(serde_json::from_reader(file)?)
                        })?;
                        accounts_account.extend(accounts);
                    }

                    let mut filters = vec![];
                    for filter in args.accounts_memcmp.iter() {
                        match filter.split_once(',') {
                            Some((offset, data)) => {
                                filters.push(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(AccountsFilterDataOneof::Memcmp(
                                        SubscribeRequestFilterAccountsFilterMemcmp {
                                            offset: offset
                                                .parse()
                                                .map_err(|_| anyhow::anyhow!("invalid offset"))?,
                                            data: Some(AccountsFilterMemcmpOneof::Base58(
                                                data.trim().to_string(),
                                            )),
                                        },
                                    )),
                                });
                            }
                            _ => anyhow::bail!("invalid memcmp"),
                        }
                    }
                    if let Some(datasize) = args.accounts_datasize {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterDataOneof::Datasize(datasize)),
                        });
                    }
                    if args.accounts_token_account_state {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterDataOneof::TokenAccountState(true)),
                        });
                    }

                    accounts.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterAccounts {
                            account: accounts_account,
                            owner: args.accounts_owner.clone(),
                            filters,
                        },
                    );
                }

                let mut slots: SlotsFilterMap = HashMap::new();
                if args.slots {
                    slots.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(args.slots_filter_by_commitment),
                        },
                    );
                }

                let mut transactions: TransactionsFilterMap = HashMap::new();
                if args.transactions {
                    transactions.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_vote,
                            failed: args.transactions_failed,
                            signature: args.transactions_signature.clone(),
                            account_include: args.transactions_account_include.clone(),
                            account_exclude: args.transactions_account_exclude.clone(),
                            account_required: args.transactions_account_required.clone(),
                        },
                    );
                }

                let mut transactions_status: TransactionsStatusFilterMap = HashMap::new();
                if args.transactions_status {
                    transactions_status.insert(
                        "client".to_string(),
                        SubscribeRequestFilterTransactions {
                            vote: args.transactions_status_vote,
                            failed: args.transactions_status_failed,
                            signature: args.transactions_status_signature.clone(),
                            account_include: args.transactions_status_account_include.clone(),
                            account_exclude: args.transactions_status_account_exclude.clone(),
                            account_required: args.transactions_status_account_required.clone(),
                        },
                    );
                }

                let mut entry: EntryFilterMap = HashMap::new();
                if args.entry {
                    entry.insert("client".to_owned(), SubscribeRequestFilterEntry {});
                }

                let mut blocks: BlocksFilterMap = HashMap::new();
                if args.blocks {
                    blocks.insert(
                        "client".to_owned(),
                        SubscribeRequestFilterBlocks {
                            account_include: args.blocks_account_include.clone(),
                            include_transactions: args.blocks_include_transactions,
                            include_accounts: args.blocks_include_accounts,
                            include_entries: args.blocks_include_entries,
                        },
                    );
                }

                let mut blocks_meta: BlocksMetaFilterMap = HashMap::new();
                if args.blocks_meta {
                    blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
                }

                let mut accounts_data_slice = Vec::new();
                for data_slice in args.accounts_data_slice.iter() {
                    match data_slice.split_once(',') {
                        Some((offset, length)) => match (offset.parse(), length.parse()) {
                            (Ok(offset), Ok(length)) => {
                                accounts_data_slice
                                    .push(SubscribeRequestAccountsDataSlice { offset, length });
                            }
                            _ => anyhow::bail!("invalid data_slice"),
                        },
                        _ => anyhow::bail!("invalid data_slice"),
                    }
                }

                let ping = args.ping.map(|id| SubscribeRequestPing { id });

                Some((
                    SubscribeRequest {
                        slots,
                        accounts,
                        transactions,
                        transactions_status,
                        entry,
                        blocks,
                        blocks_meta,
                        commitment: commitment.map(|x| x as i32),
                        accounts_data_slice,
                        ping,
                    },
                    args.resub.unwrap_or(0),
                ))
            }
            _ => None,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    is_startup: bool,
    slot: u64,
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: String,
    write_version: u64,
    txn_signature: String,
}

impl From<SubscribeUpdateAccount> for AccountPretty {
    fn from(
        SubscribeUpdateAccount {
            is_startup,
            slot,
            account,
        }: SubscribeUpdateAccount,
    ) -> Self {
        let account = account.expect("should be defined");
        Self {
            is_startup,
            slot,
            pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
            lamports: account.lamports,
            owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: hex::encode(account.data),
            write_version: account.write_version,
            txn_signature: bs58::encode(account.txn_signature.unwrap_or_default()).into_string(),
        }
    }
}

#[allow(dead_code)]
pub struct TransactionPretty {
    slot: u64,
    signature: Signature,
    is_vote: bool,
    tx: EncodedTransactionWithStatusMeta,
}

impl fmt::Debug for TransactionPretty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct TxWrap<'a>(&'a EncodedTransactionWithStatusMeta);
        impl<'a> fmt::Debug for TxWrap<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let serialized = serde_json::to_string(self.0).expect("failed to serialize");
                fmt::Display::fmt(&serialized, f)
            }
        }

        f.debug_struct("TransactionPretty")
            .field("slot", &self.slot)
            .field("signature", &self.signature)
            .field("is_vote", &self.is_vote)
            .field("tx", &TxWrap(&self.tx))
            .finish()
    }
}

impl From<SubscribeUpdateTransaction> for TransactionPretty {
    fn from(SubscribeUpdateTransaction { transaction, slot }: SubscribeUpdateTransaction) -> Self {
        let tx = transaction.expect("should be defined");
        Self {
            slot,
            signature: Signature::try_from(tx.signature.as_slice()).expect("valid signature"),
            is_vote: tx.is_vote,
            tx: yellowstone_grpc_proto::convert_from::create_tx_with_meta(tx)
                .expect("valid tx with meta")
                .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
                .expect("failed to encode"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TransactionStatusPretty {
    slot: u64,
    signature: Signature,
    is_vote: bool,
    index: u64,
    err: Option<TransactionError>,
}

impl From<SubscribeUpdateTransactionStatus> for TransactionStatusPretty {
    fn from(status: SubscribeUpdateTransactionStatus) -> Self {
        Self {
            slot: status.slot,
            signature: Signature::try_from(status.signature.as_slice()).expect("valid signature"),
            is_vote: status.is_vote,
            index: status.index,
            err: yellowstone_grpc_proto::convert_from::create_tx_error(status.err.as_ref())
                .expect("valid tx err"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        async move {
            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let commitment = args.get_commitment();
            let mut client = args.connect().await.map_err(backoff::Error::transient)?;
            info!("Connected");

            match &args.action {
                Action::HealthCheck => client
                    .health_check()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::HealthWatch => geyser_health_watch(client).await,
                Action::Subscribe(_) => {
                    let (request, resub) = args
                        .action
                        .get_subscribe_request(commitment)
                        .await
                        .map_err(backoff::Error::Permanent)?
                        .expect("expect subscribe action");

                    geyser_subscribe(client, request, resub).await
                }
                Action::Ping { count } => client
                    .ping(*count)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetLatestBlockhash => client
                    .get_latest_blockhash(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetBlockHeight => client
                    .get_block_height(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetSlot => client
                    .get_slot(commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::IsBlockhashValid { blockhash } => client
                    .is_blockhash_valid(blockhash.clone(), commitment)
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
                Action::GetVersion => client
                    .get_version()
                    .await
                    .map_err(anyhow::Error::new)
                    .map(|response| info!("response: {response:?}")),
            }
            .map_err(backoff::Error::transient)?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn geyser_health_watch(mut client: GeyserGrpcClient<impl Interceptor>) -> anyhow::Result<()> {
    let mut stream = client.health_watch().await?;
    info!("stream opened");
    while let Some(message) = stream.next().await {
        info!("new message: {message:?}");
    }
    info!("stream closed");
    Ok(())
}


struct Stats<T: Ord + Display + Default + Copy> {
    container: BTreeMap<T, usize>,
    count: usize,
}

impl<T: Ord + Display + Default + Copy> Stats<T> {
    pub fn new() -> Self {
        Self {
            container: BTreeMap::new(),
            count: 0,
        }
    }

    pub fn add_value(&mut self, value: &T) {
        self.count += 1;
        match self.container.get_mut(value) {
            Some(size) => {
                *size += 1;
            }
            None => {
                self.container.insert(*value, 1);
            }
        }
    }

    pub fn print_stats(&self, name: &str) {
        if self.count > 0 {
            let p50_index = self.count / 2;
            let p75_index = self.count * 3 / 4;
            let p90_index = self.count * 9 / 10;
            let p95_index = self.count * 95 / 100;
            let p99_index = self.count * 99 / 100;

            let mut p50_value = None::<T>;
            let mut p75_value = None::<T>;
            let mut p90_value = None::<T>;
            let mut p95_value = None::<T>;
            let mut p99_value = None::<T>;

            let mut counter = 0;
            for (value, size) in self.container.iter() {
                counter += size;
                if counter > p50_index && p50_value.is_none() {
                    p50_value = Some(*value);
                }
                if counter > p75_index && p75_value.is_none() {
                    p75_value = Some(*value);
                }
                if counter > p90_index && p90_value.is_none() {
                    p90_value = Some(*value);
                }
                if counter > p95_index && p95_value.is_none() {
                    p95_value = Some(*value);
                }
                if counter > p99_index && p99_value.is_none() {
                    p99_value = Some(*value);
                }
            }
            let max_value = self
                .container
                .last_key_value()
                .map(|x| *x.0)
                .unwrap_or_default();
            println!(
                "stats {} : p50={}, p75={}, p90={}, p95={}, p99={}, max:{}",
                name,
                p50_value.unwrap_or_default(),
                p75_value.unwrap_or_default(),
                p90_value.unwrap_or_default(),
                p95_value.unwrap_or_default(),
                p99_value.unwrap_or_default(),
                max_value
            );
        }
    }
}

#[derive(Clone, Default)]
struct ClientStats {
    pub bytes_transfered: Arc<AtomicU64>,
    pub total_accounts_size: Arc<AtomicU64>,
    pub slot_notifications: Arc<AtomicU64>,
    pub account_notification: Arc<AtomicU64>,
    pub blockmeta_notifications: Arc<AtomicU64>,
    pub transaction_notifications: Arc<AtomicU64>,
    pub block_notifications: Arc<AtomicU64>,
    pub cluster_slot: Arc<AtomicU64>,
    pub account_slot: Arc<AtomicU64>,
    pub slot_slot: Arc<AtomicU64>,
    pub blockmeta_slot: Arc<AtomicU64>,
    pub block_slot: Arc<AtomicU64>,
    pub delay: Arc<AtomicU64>,
}

async fn print_stats(client_stats: ClientStats) {

        let bytes_transfered: Arc<AtomicU64> = client_stats.bytes_transfered.clone();
        let slot_notifications = client_stats.slot_notifications.clone();
        let account_notification = client_stats.account_notification.clone();
        let blockmeta_notifications = client_stats.blockmeta_notifications.clone();
        let transaction_notifications = client_stats.transaction_notifications.clone();
        let total_accounts_size = client_stats.total_accounts_size.clone();
        let block_notifications = client_stats.block_notifications.clone();

        let cluster_slot = client_stats.cluster_slot.clone();
        let account_slot = client_stats.account_slot.clone();
        let slot_slot = client_stats.slot_slot.clone();
        let blockmeta_slot = client_stats.blockmeta_slot.clone();
        let block_slot = client_stats.block_slot.clone();
        let delay = client_stats.delay.clone();

        let mut instant = Instant::now();
        let mut bytes_transfered_stats = Stats::<u64>::new();
        let mut slot_notifications_stats = Stats::<u64>::new();
        let mut account_notification_stats = Stats::<u64>::new();
        let mut blockmeta_notifications_stats = Stats::<u64>::new();
        let mut transaction_notifications_stats = Stats::<u64>::new();
        let mut total_accounts_size_stats = Stats::<u64>::new();
        let mut block_notifications_stats = Stats::<u64>::new();
        let mut counter = 0;
        let start_instance = Instant::now();
        loop {
            counter += 1;
            tokio::time::sleep(Duration::from_secs(1) - instant.elapsed()).await;
            instant = Instant::now();
            let bytes_transfered = bytes_transfered.swap(0, std::sync::atomic::Ordering::Relaxed);
            let total_accounts_size =
                total_accounts_size.swap(0, std::sync::atomic::Ordering::Relaxed);
            let account_notification =
                account_notification.swap(0, std::sync::atomic::Ordering::Relaxed);
            let slot_notifications =
                slot_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let blockmeta_notifications =
                blockmeta_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let transaction_notifications =
                transaction_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let block_notifications =
                block_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let avg_delay_us = delay.swap(0, std::sync::atomic::Ordering::Relaxed) as f64 / account_notification as f64;
            bytes_transfered_stats.add_value(&bytes_transfered);
            total_accounts_size_stats.add_value(&total_accounts_size);
            slot_notifications_stats.add_value(&slot_notifications);
            account_notification_stats.add_value(&account_notification);
            blockmeta_notifications_stats.add_value(&blockmeta_notifications);
            transaction_notifications_stats.add_value(&transaction_notifications);
            block_notifications_stats.add_value(&block_notifications);

            log::info!("------------------------------------------");
            log::info!(
                " DateTime : {:?}",
                instant.duration_since(start_instance).as_secs()
            );
            log::info!(" Bytes Transfered : {} Mbs/s", bytes_transfered / 1_000_000);
            log::info!(
                " Accounts transfered size (uncompressed) : {} Mbs",
                total_accounts_size / 1_000_000
            );
            log::info!(" Accounts Notified : {}", account_notification);
            log::info!(" Slots Notified : {}", slot_notifications);
            log::info!(" Blockmeta notified : {}", blockmeta_notifications);
            log::info!(" Transactions notified : {}", transaction_notifications);
            log::info!(" Blocks notified : {}", block_notifications);
            log::info!(" Average delay by accounts : {:.02} ms", avg_delay_us / 1000.0);


            log::info!(" Cluster Slots: {}, Account Slot: {}, Slot Notification slot: {}, BlockMeta slot: {}, Block slot: {}", cluster_slot.load(std::sync::atomic::Ordering::Relaxed), account_slot.load(std::sync::atomic::Ordering::Relaxed), slot_slot.load(std::sync::atomic::Ordering::Relaxed), blockmeta_slot.load(std::sync::atomic::Ordering::Relaxed), block_slot.load(std::sync::atomic::Ordering::Relaxed));

            if counter % 10 == 0 {
                println!("------------------STATS------------------------");
                bytes_transfered_stats.print_stats("Bytes transfered");
                total_accounts_size_stats.print_stats("Total account size uncompressed");
                slot_notifications_stats.print_stats("Slot Notifications");
                account_notification_stats.print_stats("Account notifications");
                blockmeta_notifications_stats.print_stats("Block meta Notifications");
                transaction_notifications_stats.print_stats("Transaction notifications");
                block_notifications_stats.print_stats("Block Notifications");
            }
        }
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
    resub: usize,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("stream opened");
    let client_stats = ClientStats::default();
    {
        let client_stats = client_stats.clone();
        tokio::spawn(async move {print_stats(client_stats).await});
    }
    let mut counter = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Account(account)) => {
                        let slot = account.slot;
                        if let Some(account) = account.account{
                            client_stats
                            .account_notification
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let data_len = account.data.len() as u64;
                            client_stats
                                .total_accounts_size
                                .fetch_add(data_len, std::sync::atomic::Ordering::Relaxed);
                            client_stats.bytes_transfered.fetch_add(data_len, std::sync::atomic::Ordering::Relaxed);
                            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64;
                            let account_in_us = account.lamports;
                            client_stats.delay.fetch_add( now.saturating_sub(account_in_us), std::sync::atomic::Ordering::Relaxed);
                            client_stats.account_slot.store(
                                slot,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        continue;
                    }
                    Some(UpdateOneof::Transaction(tx)) => {
                        let tx: TransactionPretty = tx.into();
                        info!(
                            "new transaction update: filters {:?}, transaction: {:#?}",
                            msg.filters, tx
                        );
                        continue;
                    }
                    Some(UpdateOneof::TransactionStatus(status)) => {
                        let status: TransactionStatusPretty = status.into();
                        info!(
                            "new transaction update: filters {:?}, transaction status: {:?}",
                            msg.filters, status
                        );
                        continue;
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                        // require periodic client pings then this is unnecessary
                        subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await?;
                    }
                    _ => {}
                }
                info!("new message: {msg:?}")
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }

        // Example to illustrate how to resubscribe/update the subscription
        counter += 1;
        if counter == resub {
            let mut new_slots: SlotsFilterMap = HashMap::new();
            new_slots.insert("client".to_owned(), SubscribeRequestFilterSlots::default());

            subscribe_tx
                .send(SubscribeRequest {
                    slots: new_slots.clone(),
                    accounts: HashMap::default(),
                    transactions: HashMap::default(),
                    transactions_status: HashMap::default(),
                    entry: HashMap::default(),
                    blocks: HashMap::default(),
                    blocks_meta: HashMap::default(),
                    commitment: None,
                    accounts_data_slice: Vec::default(),
                    ping: None,
                })
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
    }
    info!("stream closed");
    Ok(())
}
