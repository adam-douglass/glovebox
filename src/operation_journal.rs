use std::{io::SeekFrom, path::PathBuf};
use std::path::Path;

use bincode::Options;
use log::error;
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};

use crate::data_journal::file_exists;
use crate::error::Error;

#[derive(Serialize, Deserialize)]
struct OperationJournalHeader {
    id: u32
}

#[derive(Serialize, Deserialize)]
enum OperationJournalHeaderData {
    V0(OperationJournalHeader),
}

static MAX_HEADER_SIZE: usize = 128;

impl OperationJournalHeaderData  {
    fn current(self) -> OperationJournalHeader {
        match self {
            OperationJournalHeaderData::V0(data) => data,
        }
    }
}

pub struct OperationJournal<Event> {
    pub journal: tokio::fs::File,
    header_size: u64,
    journal_path: PathBuf,
    _event: std::marker::PhantomData<Event>,
}

impl<'a, Event: Serialize + DeserializeOwned> OperationJournal<Event> {

    pub async fn exists(path: &PathBuf, id: u32) -> anyhow::Result<bool> {
        let path = path.join(id.to_string() + "_meta");
        Ok(file_exists(&path).await?)
    }

    pub async fn erase(mut self) -> anyhow::Result<()> {
        let _ = self.journal.flush().await;
        drop(self.journal);
        tokio::fs::remove_file(self.journal_path).await?;
        Ok(())
    }

    pub async fn open(path: &Path, id: u32) -> anyhow::Result<Self> {
        let path = path.join(id.to_string() + "_meta");
        let existing_journal = file_exists(&path).await?;
        let mut journal = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .await?;

        let encoder = bincode::DefaultOptions::new()
            .allow_trailing_bytes()
            .with_varint_encoding();

        let header_size = if existing_journal {
            journal.seek(SeekFrom::Start(0)).await?;

            let mut data = vec![];
            data.reserve(MAX_HEADER_SIZE);
            journal.read_buf(&mut data).await?;

            let header_data: OperationJournalHeaderData = encoder.deserialize(&data)?;
            let header_size = encoder.serialized_size(&header_data)?;
            let header = header_data.current();
            
            if header.id != id {
                return Err(Error::JournalHeaderIdError(path, header.id, id).into())
            }
            journal.seek(SeekFrom::Start(header_size)).await?;

            header_size
        } else {
            let header = OperationJournalHeaderData::V0(OperationJournalHeader{id});
            let header = encoder.serialize(&header)?;
            journal.write_all(&header).await?;
            header.len() as u64
        };
        
        Ok(Self {journal, header_size, journal_path: path, _event: Default::default()})
    }

    pub async fn append(&mut self, event: &Event) -> anyhow::Result<()> {
        let data = bincode::serialize(event)?;
        self.journal.write_u32_le(data.len() as u32).await?;
        self.journal.write_all(&data).await?;
        Ok(())
    }

    pub async fn read_all(&mut self) -> anyhow::Result<Vec<(u64, Event)>> {
        let mut out = vec![];
        let mut offset = self.header_size;
        self.journal.seek(SeekFrom::Start(offset)).await?;
        loop {
            let length = match self.journal.read_u32_le().await {
                Ok(value) => value,
                Err(err) => if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break
                } else {
                    return Err(err.into())
                },
            } as u64;
            let mut data = vec![0; length as usize];
            match self.journal.read_exact(&mut data).await {
                Ok(read) => if read != length as usize {
                    break
                },
                Err(err) => if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break
                } else {
                    return Err(err.into())
                },
            };
            offset += length + 4;
            out.push(match bincode::deserialize(&data){
                Ok(value) => (offset, value),
                Err(_) => continue,
            });
        }
        let len = self.journal.seek(SeekFrom::End(0)).await?;
        if len != offset {
            error!("Journal truncated, probably incomplete write");
            self.journal.set_len(offset).await?;
        }
        return Ok(out);
    }

    pub async fn sync(&mut self) -> anyhow::Result<()> {
        Ok(self.journal.sync_all().await?)
    }

    pub async fn truncate(&mut self, size: u64) -> anyhow::Result<()> {
        Ok(self.journal.set_len(size).await?)
    }
}

unsafe impl<T> Send for OperationJournal<T> where T: Send {}
