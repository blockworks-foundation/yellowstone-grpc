use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::time::{Instant, SystemTime};
use log::{debug, info, warn};
use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::error::SendError;
use {
    crate::{
        config::Config,
        grpc::{GrpcService, Message},
        prom::{self, PrometheusService, MESSAGE_QUEUE_SIZE},
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
        SlotStatus,
    },
    std::{
        concat, env,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::{
        runtime::{Builder, Runtime},
        sync::{mpsc, Notify},
    },
};
use crate::grpc::{MessageAccount, MessageAccountInfo};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    snapshot_channel: Option<crossbeam_channel::Sender<Option<Message>>>,
    grpc_channel: mpsc::UnboundedSender<Message>,
    grpc_shutdown: Arc<Notify>,
    prometheus: PrometheusService,
}

impl PluginInner {
    fn send_message(&self, message: Message) {
        match self.grpc_channel.send(message) {
            Ok(()) => {
            MESSAGE_QUEUE_SIZE.inc();
            }
            Err(_send_error) => {
                warn!("failed to send message to grpc channel: channel closed");
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner) -> PluginResult<()>,
    {
        let inner = self.inner.as_ref().expect("initialized");
        f(inner)
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        solana_logger::setup_with_default(&config.log.level);

        // Create inner
        let runtime = Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("solGeyserGrpc{id:02}")
            })
            .enable_all()
            .build()
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        let (snapshot_channel, grpc_channel, grpc_shutdown, prometheus) =
            runtime.block_on(async move {
                let (snapshot_channel, grpc_channel, grpc_shutdown) =
                    GrpcService::create(config.grpc, config.block_fail_action)
                        .await
                        .map_err(GeyserPluginError::Custom)?;
                let prometheus = PrometheusService::new(config.prometheus)
                    .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;
                Ok::<_, GeyserPluginError>((
                    snapshot_channel,
                    grpc_channel,
                    grpc_shutdown,
                    prometheus,
                ))
            })?;

        self.inner = Some(PluginInner {
            runtime,
            snapshot_channel,
            grpc_channel,
            grpc_shutdown,
            prometheus,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.grpc_shutdown.notify_one();
            drop(inner.grpc_channel);
            inner.prometheus.shutdown();
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };

            if is_startup {
                let message = Message::Account((account, slot, is_startup).into());
                if let Some(channel) = &inner.snapshot_channel {
                    match channel.send(Some(message)) {
                        Ok(()) => MESSAGE_QUEUE_SIZE.inc(),
                        Err(_) => panic!("failed to send message to startup queue: channel closed"),
                    }
                }
            } else {

                // let message = if account.data.len() > 1_000 {
                //     let started_at = Instant::now();
                //     let compressed_data = lz4_flex::compress_prepend_size(&account.data);
                //     let elapsed_us = started_at.elapsed().as_micros();
                //     let replica = ReplicaAccountInfoV3 {
                //         data: &compressed_data,
                //         ..account.clone()
                //     };
                //         let pubkey = Pubkey::try_from(account.pubkey).unwrap();
                //         info!("LZ4 compressed account data in {}us: {} -> {} of {}",
                //             elapsed_us, account.data.len(), compressed_data.len(), pubkey);
                //
                //     Message::Account((&replica, slot, is_startup).into())
                // } else {
                //     Message::Account((account, slot, is_startup).into())
                // };

                let now = SystemTime::now();
                let since_the_epoch = now.duration_since(SystemTime::UNIX_EPOCH).expect("Time went backwards");

                info!("account update inspect from geyser: write_version={};timestamp_us={};slot={}",
                                                    account.write_version, since_the_epoch.as_micros(), slot);

                let message = Message::Account((account, slot, is_startup).into());

                inner.send_message(message);
            }

            Ok(())
        })
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        self.with_inner(|inner| {
            if let Some(channel) = &inner.snapshot_channel {
                match channel.send(None) {
                    Ok(()) => MESSAGE_QUEUE_SIZE.inc(),
                    Err(_) => panic!("failed to send message to startup queue: channel closed"),
                }
            }
            Ok(())
        })
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let message = Message::Slot((slot, parent, status).into());
            inner.send_message(message);
            prom::update_slot_status(status, slot);
            Ok(())
        })
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let transaction = match transaction {
                ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_2(info) => info,
            };

            let message = Message::Transaction((transaction, slot).into());
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        self.with_inner(|inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let entry = match entry {
                ReplicaEntryInfoVersions::V0_0_1(entry) => entry,
            };

            let message = Message::Entry(entry.into());
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        self.with_inner(|inner| {
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_3(info) => info,
            };

            let message = Message::BlockMeta(blockinfo.into());
            inner.send_message(message);

            Ok(())
        })
    }

    fn account_data_notifications_enabled(&self) -> bool {
        // warn!("!!! DISABLE ACCOUNT NOTIFICATIONS - for testing");
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
