pub mod broker;
pub mod shard;
pub mod entry;

// #[derive(Debug)]
// pub enum ClientMessageType {
//     Ready,
//     Write,
//     Sync,
//     Assign,
//     Finish,
//     Retry,
//     Drop,
//     SendEntry(self::entry::Entry, bool),
//     NoEntry,
// }

use crate::request::NotificationName;
use crate::response::{ClientResponse, ClientNotice};

use self::entry::ClientId;

#[derive(Debug)]
pub struct ClientMessage {
    pub client: entry::ClientId,
    pub message: ClientResponse,
}

impl ClientMessage {
    pub fn note(client: ClientId, label: u64, notice: NotificationName) -> Self {
        Self {
            client, 
            message: ClientResponse::Notice(ClientNotice{label, notice})
        }
    }
}