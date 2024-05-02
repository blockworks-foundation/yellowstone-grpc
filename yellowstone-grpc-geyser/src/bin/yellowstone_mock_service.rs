use std::thread::{sleep, spawn};
use std::time::Duration;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_sdk::recent_blockhashes_account::update_account;
use yellowstone_grpc_geyser::config::{ConfigBlockFailAction, ConfigGrpc, ConfigGrpcFilters};
use yellowstone_grpc_geyser::grpc::{GrpcService, Message, MessageAccount, MessageAccountInfo};

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

    let (snapshot_channel, grpc_channel, grpc_shutdown) =
        GrpcService::create(config_grpc, ConfigBlockFailAction::Panic)
            .await
            .unwrap();

    tokio::spawn(async move {

        loop {
            let update_account = MessageAccount {
                account: MessageAccountInfo {
                    pubkey: Default::default(),
                    lamports: 0,
                    owner: Default::default(),
                    executable: false,
                    rent_epoch: 0,
                    data: vec![1,2,3],
                    write_version: 0,
                    txn_signature: None,
                },
                slot: 999_999,
                is_startup: false,
            };

            grpc_channel.send(Message::Account(update_account)).expect("send");
            println!("sent account update down the stream");

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

    });


    sleep(Duration::from_secs(10000));
}