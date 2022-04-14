
use std::path::PathBuf;

use serde::{Deserialize, Serialize};


#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)] 
pub struct QueueConfiguration {
    pub name: String,
    #[serde(default="default_max_retries")]
    pub max_retries: u32,
    #[serde(default="default_max_records")]
    pub shard_max_records: u64,
    #[serde(default="default_max_bytes")]
    pub shard_max_bytes: u64,
}

fn default_max_retries() -> u32 { u32::MAX }
fn default_max_records() -> u64 { 1 << 16 }
fn default_max_bytes() -> u64 { 1 << 30 }


#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)] 
pub struct Configuration {
    #[serde(default="default_data_path")]
    pub data_path: PathBuf,
    #[serde(default="default_port")]
    pub port: u16,
    #[serde(default="default_bind_address")]
    pub bind_address: String,
    #[serde(default="default_compression")]
    pub compression: u8,
    #[serde(default="default_queue_create")]
    pub runtime_create_queues: bool,
    #[serde(default)]
    pub queues: Vec<QueueConfiguration>,
}

fn default_data_path() -> PathBuf {
    let app_dirs = platform_dirs::AppDirs::new(Some("glovebox"), true).unwrap();
    app_dirs.data_dir
}
fn default_port() -> u16 { 80 }
fn default_bind_address() -> String { String::from("127.0.0.1") }
fn default_compression() -> u8 { 9 }
fn default_queue_create() -> bool { true }

impl Default for Configuration {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}