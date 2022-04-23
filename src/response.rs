use serde::{Serialize, Deserialize};

// use crate::error::Error;
// use crate::priority::{ClientMessage, ClientMessageType};
use crate::request::{NotificationName, MessageJSON, message_encoder};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientDeliveryJSON {
    pub label: u64,
    pub shard: u32,
    pub sequence: u64,
    #[serde(serialize_with="message_encoder")]
    pub body: MessageJSON,
    pub finished: bool,
}

impl From<ClientDelivery> for ClientDeliveryJSON {
    fn from(del: ClientDelivery) -> Self {
        Self {
            label: del.label,
            shard: del.shard,
            sequence: del.sequence,
            body: MessageJSON::Binary(del.body),
            finished: del.finished,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientDelivery {
    pub label: u64,
    pub shard: u32,
    pub sequence: u64,
    pub body: Vec<u8>,
    pub finished: bool,
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientNotice {
    pub label: u64,
    pub notice: NotificationName
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientHello {
    pub id: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientNoMessage {
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ErrorCode {
    NoObject,
    ObjectAlreadyExists,
    PermissionDenied
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientError {
    pub label: u64,
    pub code: ErrorCode,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag="type", rename_all="lowercase")]
pub enum ClientResponseJSON {
    Message(ClientDeliveryJSON),
    Notice(ClientNotice),
    Hello(ClientHello),
    NoMessage(ClientNoMessage),
    Error(ClientError),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ClientResponseBinV0 {
    Message(ClientDelivery),
    Notice(ClientNotice),
    Hello(ClientHello),
    NoMessage(ClientNoMessage),
    Error(ClientError),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ClientResponseBin {
    V0(ClientResponseBinV0)
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClientResponse {
    Message(ClientDelivery),
    Notice(ClientNotice),
    Hello(ClientHello),
    NoMessage(ClientNoMessage),
    Error(ClientError),
}

// impl TryFrom<ClientMessage> for ClientResponse {
//     type Error = Error;

//     // <ClientResponse as TryFrom<ClientMessage>>::
//     fn try_from(note: ClientMessage) -> Result<ClientResponse, Error> {
//         Ok(match note.notice {
//             ClientMessageType::Ready => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Ready }),
//             ClientMessageType::Write => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Write }),
//             ClientMessageType::Sync => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Sync }),
//             ClientMessageType::Assign => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Assign }),
//             ClientMessageType::Finish => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Finish }),
//             ClientMessageType::Retry => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Retry }),
//             ClientMessageType::Drop => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Drop }),
//             ClientMessageType::SendEntry(entry, finished) => {
//                 let body = match std::sync::Arc::try_unwrap(entry.body) {
//                     Ok(body) => body,
//                     Err(err) => err.as_ref().clone(),
//                 };

//                 ClientResponse::Message(ClientDelivery {
//                     label: note.label,
//                     shard: entry.header.shard,
//                     sequence: entry.header.sequence.0,
//                     body: body,
//                     finished,
//                 })
//             },
//             ClientMessageType::NoEntry => ClientResponse::NoMessage(ClientNoMessage{
//                 label: note.label,
//             })
//         })
//     }
// }

impl From<ClientResponse> for ClientResponseJSON {
    fn from(resp: ClientResponse) -> Self {
        match resp {
            ClientResponse::Message(v) => ClientResponseJSON::Message(v.into()),
            ClientResponse::Notice(v) => ClientResponseJSON::Notice(v),
            ClientResponse::Hello(v) => ClientResponseJSON::Hello(v),
            ClientResponse::NoMessage(v) => ClientResponseJSON::NoMessage(v),
            ClientResponse::Error(v) => ClientResponseJSON::Error(v),
        }
    }
}

impl From<ClientResponse> for ClientResponseBin {
    fn from(resp: ClientResponse) -> Self {
        ClientResponseBin::V0(match resp {
            ClientResponse::Message(v) => ClientResponseBinV0::Message(v),
            ClientResponse::Notice(v) => ClientResponseBinV0::Notice(v),
            ClientResponse::Hello(v) => ClientResponseBinV0::Hello(v),
            ClientResponse::NoMessage(v) => ClientResponseBinV0::NoMessage(v),
            ClientResponse::Error(v) => ClientResponseBinV0::Error(v),
        })
    }
}

impl From<ClientResponseBin> for ClientResponse {
    fn from(resp: ClientResponseBin) -> Self {
        match resp {
            ClientResponseBin::V0(resp) => match resp {
                ClientResponseBinV0::Message(v) => ClientResponse::Message(v),
                ClientResponseBinV0::Notice(v) => ClientResponse::Notice(v),
                ClientResponseBinV0::Hello(v) => ClientResponse::Hello(v),
                ClientResponseBinV0::NoMessage(v) => ClientResponse::NoMessage(v),
                ClientResponseBinV0::Error(v) => ClientResponse::Error(v),
            },
        }
    }
}