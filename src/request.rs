use serde::{Serialize, Deserialize, Serializer};

use crate::priority::entry::Firmness;
use crate::session::NotificationMask;


#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum MessageJSON {
    Binary(Vec<u8>),
    String(String),
}

impl From<String> for MessageJSON {
    fn from(value: String) -> Self {
        MessageJSON::String(value)
    }
}

impl From<Vec<u8>> for MessageJSON {
    fn from(value: Vec<u8>) -> Self {
        MessageJSON::Binary(value)
    }
}

impl MessageJSON {
    pub fn bytes(self) -> Vec<u8> {
        match self {
            MessageJSON::Binary(value) => value,
            MessageJSON::String(value) => value.as_bytes().to_vec(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientPostJSON {
    pub queue: String,
    #[serde(serialize_with="message_encoder")]
    pub message: MessageJSON,
    #[serde(default="default_priority")]
    pub priority: i16,
    #[serde(default="default_label")]
    pub label: u64,
    #[serde(default)]
    pub notify: Vec<NotificationName>,
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientPost {
    pub queue: String,
    pub message: Vec<u8>,
    #[serde(default="default_priority")]
    pub priority: i16,
    #[serde(default="default_label")]
    pub label: u64,
    #[serde(default)]
    pub notify: Vec<NotificationName>,
}

impl From<ClientPostJSON> for ClientPost {
    fn from(post: ClientPostJSON) -> Self {
        ClientPost {
            queue: post.queue,
            message: post.message.bytes(),
            priority: post.priority,
            label: post.label,
            notify: post.notify,
        }
    }
}

impl From<ClientPost> for ClientPostJSON {
    fn from(post: ClientPost) -> Self {
        ClientPostJSON {
            queue: post.queue,
            message: MessageJSON::Binary(post.message),
            priority: post.priority,
            label: post.label,
            notify: post.notify,
        }
    }
}

pub fn message_encoder<S>(data: &MessageJSON, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
    match data {
        MessageJSON::Binary(data) => {
            if serializer.is_human_readable() {
                match std::str::from_utf8(&data) {
                    Ok(string) => serializer.serialize_str(string),
                    Err(_) => serializer.serialize_bytes(&data),
                }
            } else {
                serializer.serialize_bytes(&data)
            }
        }
        MessageJSON::String(data) => {
            if serializer.is_human_readable() {
                serializer.serialize_str(data)
            } else {
                serializer.serialize_bytes(&data.as_bytes())
            }
        },
    }
}

pub fn default_priority() -> i16 { 0 }
pub fn default_label() -> u64 { 0 }

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientFetch {
    pub queue: String,
    pub blocking: bool,
    pub block_timeout: f32,
    pub work_timeout: f32,
    pub sync: Firmness,
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientFinish {
    pub queue: String,
    pub sequence: u64,
    pub shard: u32,
    #[serde(default)]
    pub label: Option<u64>,
    #[serde(default)]
    pub response: Option<Firmness>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientPop {
    pub queue: String,
    pub sync: Firmness,
    pub timeout: Option<f32>,
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ClientCreate {
    pub queue: String,
    pub label: u64,
    pub retries: Option<u32>,
    pub shard_max_entries: Option<u64>,
    pub shard_max_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag="type", rename_all="lowercase")]
pub enum ClientRequestJSON {
    Post(ClientPostJSON),
    Fetch(ClientFetch),
    Finish(ClientFinish),
    Pop(ClientPop),
    Create(ClientCreate),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ClientRequestBinV0 {
    Post(ClientPost),
    Fetch(ClientFetch),
    Finish(ClientFinish),
    Pop(ClientPop),
    Create(ClientCreate),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ClientRequestBin {
    V0(ClientRequestBinV0)
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClientRequest {
    Post(ClientPost),
    Fetch(ClientFetch),
    Finish(ClientFinish),
    Pop(ClientPop),
    Create(ClientCreate),
}

impl From<ClientRequest> for ClientRequestJSON {
    fn from(value: ClientRequest) -> Self {
        match value {
            ClientRequest::Post(v) => ClientRequestJSON::Post(v.into()),
            ClientRequest::Fetch(v) => ClientRequestJSON::Fetch(v),
            ClientRequest::Finish(v) => ClientRequestJSON::Finish(v),
            ClientRequest::Pop(v) => ClientRequestJSON::Pop(v),
            ClientRequest::Create(v) => ClientRequestJSON::Create(v),
        }
    }
}

impl From<ClientRequestJSON> for ClientRequest {
    fn from(value: ClientRequestJSON) -> Self {
        match value {
            ClientRequestJSON::Post(v) => ClientRequest::Post(v.into()),
            ClientRequestJSON::Fetch(v) => ClientRequest::Fetch(v),
            ClientRequestJSON::Finish(v) => ClientRequest::Finish(v),
            ClientRequestJSON::Pop(v) => ClientRequest::Pop(v),
            ClientRequestJSON::Create(v) => ClientRequest::Create(v),
        }
    }
}


impl From<ClientRequest> for ClientRequestBin {
    fn from(value: ClientRequest) -> Self {
        ClientRequestBin::V0(match value {
            ClientRequest::Post(v) => ClientRequestBinV0::Post(v),
            ClientRequest::Fetch(v) => ClientRequestBinV0::Fetch(v),
            ClientRequest::Finish(v) => ClientRequestBinV0::Finish(v),
            ClientRequest::Pop(v) => ClientRequestBinV0::Pop(v),
            ClientRequest::Create(v) => ClientRequestBinV0::Create(v),
        })
    }
}

impl From<ClientRequestBin> for ClientRequest {
    fn from(value: ClientRequestBin) -> Self {
        match value {
            ClientRequestBin::V0(value) => match value {
                ClientRequestBinV0::Post(v) => ClientRequest::Post(v),
                ClientRequestBinV0::Fetch(v) => ClientRequest::Fetch(v),
                ClientRequestBinV0::Finish(v) => ClientRequest::Finish(v),
                ClientRequestBinV0::Pop(v) => ClientRequest::Pop(v),
                ClientRequestBinV0::Create(v) => ClientRequest::Create(v),
            }
        }
    }
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

    use super::{ClientRequest, ClientPost, ClientRequestJSON, ClientRequestBin};

    #[tokio::test]
    async fn parse_post() {
        let value: ClientRequestJSON = serde_json::from_str(r#"{"type": "post", "queue": "abc", "message": "hello"}"#).unwrap();
        let value: ClientRequest = value.into();
        if let ClientRequest::Post(post) = &value {
            assert_eq!(post, &ClientPost{ 
                queue: "abc".to_string(), 
                message: String::from("hello").into_bytes(), 
                priority: default_priority(), 
                label: default_label(), 
                notify: vec![] 
            });
        } else {
            assert!(false);
        }
        let encoded = serde_json::to_string(&ClientRequestJSON::from(value)).unwrap();
        assert!(encoded.contains(r#""type":"post""#));
        assert!(encoded.contains(r#""message":"hello""#));
        assert!(encoded.contains(r#""queue":"abc""#));
    }

    #[tokio::test]
    async fn parse_binary_post() {
        let value: ClientRequestJSON = serde_json::from_str(r#"{"type": "post", "queue": "abc", "message": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 255]}"#).unwrap();
        let value: ClientRequest = value.into();
        if let ClientRequest::Post(post) = &value {
            assert_eq!(post, &ClientPost{ 
                queue: "abc".to_string(), 
                message: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 255], 
                priority: default_priority(), 
                label: default_label(), 
                notify: vec![] 
            });
        } else {
            assert!(false);
        }
        let encoded = serde_json::to_string(&ClientRequestJSON::from(value.clone())).unwrap();
        println!("{}", encoded);
        assert!(encoded.contains(r#""type":"post""#));
        assert!(encoded.contains(r#""message":[0,1,2,3,4,5,6,7,8,9,10,255]"#));
        assert!(encoded.contains(r#""queue":"abc""#));

        let second_value: ClientRequestBin = bincode::deserialize(&bincode::serialize(&ClientRequestBin::from(value.clone())).unwrap()).unwrap();
        let second_value: ClientRequest = second_value.into();
        assert_eq!(second_value, value);
    }
}