#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod config;
pub mod filters;
pub mod grpc;
pub mod plugin;
pub mod prom;
pub mod version;
pub mod histogram_stats_calculation;

// log every X account write
pub const THROTTLE_ACCOUNT_LOGGING: u64 = 50;
