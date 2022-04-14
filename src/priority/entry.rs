use serde::{Serialize, Deserialize};

use crate::session::NotificationMask;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]

pub struct ClientId(pub u64);

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}


#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNo(pub u64);

// impl SequenceNo {
//     pub fn as_u64(&self) -> u64 { self.0 }
// }


#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Firmness {
    Ready,
    Write,
    Sync,
}


// #[serde_as]
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct NoticeRequest {
    pub label: u64,
    pub client: ClientId,
    #[serde(with = "mask_encoding")]
    // #[serde_as(as = "u8")]
    pub notices: NotificationMask
}

mod mask_encoding {
    use serde::{Serializer, Deserializer};
    use serde::de::Deserialize;
    use crate::session::NotificationMask;

    pub fn serialize<S>(notice: &NotificationMask, ser: S) -> Result<S::Ok, S::Error> where S: Serializer {
        ser.serialize_u8(u8::from(*notice))
    }

    pub fn deserialize<'de, D>(der: D) -> Result<NotificationMask, D::Error> where D: Deserializer<'de> {
        let num = u8::deserialize(der)?;
        Ok(NotificationMask::from(num))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Assignment {
    pub expiry: chrono::DateTime<chrono::Utc>,
    pub client: ClientId,
    pub notice: NoticeRequest
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntryHeader {
    pub shard: u32,
    pub length: u32,
    pub location: u64,
    pub priority: u32,
    pub attempts: u32,
    pub sequence: SequenceNo,
    pub notice: NoticeRequest,
}

impl PartialEq for EntryHeader {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl Eq for EntryHeader {

}

impl Ord for EntryHeader {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority).then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for EntryHeader {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// pub enum EntryHeaderVariant<'a> {
//     Real(EntryHeader),
//     Ref(&'a EntryHeader)
// }

// impl<'a> EntryHeaderVariant<'a> {
//     fn real(self) -> EntryHeader {
//         match self {
//             EntryHeaderVariant::Real(entry) => entry,
//             EntryHeaderVariant::Ref(entry) => entry.clone(),
//         }
//     }

//     fn reference(&'a self) -> &'a EntryHeader {
//         match self {
//             EntryHeaderVariant::Real(entry) => entry,
//             EntryHeaderVariant::Ref(entry) => entry,
//         }
//     }
// }

#[derive(Debug)]
pub struct Entry {
    pub header: EntryHeader,
    pub body: std::sync::Arc<Vec<u8>>
}

#[derive(Serialize, Deserialize)]
pub enum EntryChange {
    New(EntryHeader, u32),
    Assign(ClientId, SequenceNo, chrono::DateTime<chrono::Utc>),
    AssignTimeout(ClientId, SequenceNo, chrono::DateTime<chrono::Utc>),
    Finish(ClientId, SequenceNo),
    Pop(SequenceNo)
}

