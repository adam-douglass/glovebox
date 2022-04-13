pub mod broker;
pub mod shard;
pub mod entry;


pub enum ClientMessageType {
    Ready,
    Write,
    Sync,
    Assign,
    Finish,
    Retry,
    Drop,
    SendEntry(self::entry::Entry, bool),
    NoEntry,
}

pub struct ClientMessage {
    pub client: entry::ClientId,
    pub label: u64,
    pub notice: ClientMessageType,
}