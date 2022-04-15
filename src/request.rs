use chrono::serde::ts_milliseconds::serialize;
use serde::{Serialize, Deserialize, Serializer};
use serde_with::serde_as;

use crate::priority::entry::Firmness;
use crate::session::NotificationMask;


#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NotificationName {
    Ready,
    Write,
    Sync,
    Assign,
    Finish,
    Retry,
    Drop
}


impl From<&NotificationName> for NotificationMask {
    fn from(value: &NotificationName) -> Self {
        match value {
            NotificationName::Ready => NotificationMask::Ready,
            NotificationName::Write => NotificationMask::Write,
            NotificationName::Sync => NotificationMask::Sync,
            NotificationName::Assign => NotificationMask::Assign,
            NotificationName::Finish => NotificationMask::Finish,
            NotificationName::Retry => NotificationMask::Retry,
            NotificationName::Drop => NotificationMask::Drop,
        }
    }
}


impl From<&Vec<NotificationName>> for NotificationMask {
    fn from(fields: &Vec<NotificationName>) -> Self {
        let mut out: Self = Self::none();
        for value in fields.iter() {
            out |= NotificationMask::from(value);
        }
        out
    }
}

// #[derive(Serialize, Deserialize, Debug, PartialEq)]
// #[serde(tag = "encoding", content = "message", rename_all = "lowercase")]
// pub enum Message { 
//     Text(String),
//     Base64(String),
//     Bytes(Vec<u8>)
// }

// impl TryInto<Vec<u8>> for Message {
//     type Error = base64::DecodeError;

//     fn try_into(self) -> Result<Vec<u8>, Self::Error> {
//         Ok(match self {
//             Message::Text(data) => data.as_bytes().to_vec(),
//             Message::Base64(data) => base64::decode(data)?,
//             Message::Bytes(data) => data,
//         })
//     }
// }


#[serde_as]
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ClientPost {
    pub queue: String,
    #[serde(serialize_with="message_encoder", deserialize_with="serde_with::rust::bytes_or_string::deserialize")]
    pub message: Vec<u8>,
    #[serde(default="default_priority")]
    pub priority: i16,
    #[serde(default="default_label")]
    pub label: u64,
    #[serde(default)]
    pub notify: Vec<NotificationName>,
}

fn message_encoder<S>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
    if serializer.is_human_readable() {
        match std::str::from_utf8(&data) {
            Ok(string) => serializer.serialize_str(string),
            Err(_) => serializer.serialize_bytes(&data),
        }
    } else {
        serializer.serialize_bytes(&data)
    }
}

pub fn default_priority() -> i16 { 0 }
pub fn default_label() -> u64 { 0 }

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientFetch {
    pub queue: String,
    pub blocking: bool,
    pub block_timeout: f32,
    pub work_timeout: f32,
    pub sync: Firmness,
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientFinish {
    pub queue: String,
    pub sequence: u64,
    pub shard: u32,
    #[serde(default)]
    pub label: Option<u64>,
    #[serde(default)]
    pub response: Option<Firmness>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientPop {
    pub queue: String,
    pub sync: Firmness,
    pub blocking: bool,
    pub block_timeout: f32,
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientCreate {
    pub queue: String,
    pub label: u64,
    pub retries: Option<u32>,
    pub shard_max_entries: Option<u64>,
    pub shard_max_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag="type", rename_all="lowercase")]
pub enum ClientRequest {
    Post(ClientPost),
    Fetch(ClientFetch),
    Finish(ClientFinish),
    Pop(ClientPop),
    Create(ClientCreate),
}

impl ClientRequest {
    pub fn label(&self) -> u64 {
        match self {
            ClientRequest::Post(post) => post.label,
            ClientRequest::Fetch(fetch) => fetch.label,
            ClientRequest::Finish(finish) => finish.label.unwrap_or(0),
            ClientRequest::Pop(pop) => pop.label,
            ClientRequest::Create(create) => create.label,
        }
    }

    pub fn queue(&self) -> String {
        match self {
            ClientRequest::Post(post) => post.queue.clone(),
            ClientRequest::Fetch(fetch) => fetch.queue.clone(),
            ClientRequest::Finish(finish) => finish.queue.clone(),
            ClientRequest::Pop(pop) => pop.queue.clone(),
            ClientRequest::Create(create) => create.queue.clone()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::request::{default_priority, default_label};

    use super::{ClientRequest, ClientPost};

    #[tokio::test]
    async fn parse_post() {
        let value: ClientRequest = serde_json::from_str(r#"{"type": "post", "queue": "abc", "message": "hello"}"#).unwrap();
        if let ClientRequest::Post(post) = &value {
            assert_eq!(post, &ClientPost{ 
                queue: "abc".to_string(), 
                message: "hello".as_bytes().to_vec(), 
                priority: default_priority(), 
                label: default_label(), 
                notify: vec![] 
            });
        } else {
            assert!(false);
        }
        let encoded = serde_json::to_string(&value).unwrap();
        assert!(encoded.contains(r#""type":"post""#));
        assert!(encoded.contains(r#""message":"hello""#));
        assert!(encoded.contains(r#""queue":"abc""#));
    }
}