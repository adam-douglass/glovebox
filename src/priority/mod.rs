pub mod broker;
pub mod shard;
pub mod entry;

use crate::request::NotificationName;
use crate::response::{ClientResponse, ClientNotice, ClientNoMessage};

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

    pub fn no_entry(client: ClientId, label: u64) -> Self {
        Self {
            client, 
            message: ClientResponse::NoMessage(ClientNoMessage{
                label,
            })
        }
    }
}