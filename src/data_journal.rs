use std::path::{Path, PathBuf};
use std::io::SeekFrom;
use bincode::Options;
use log::info;
use serde::{Serialize, Deserialize};
use tokio::{io::{AsyncSeekExt, AsyncWriteExt, AsyncReadExt}};

use crate::error::Error;

#[derive(Serialize, Deserialize)]
struct DataJournalHeader {
    id: u32
}

#[derive(Serialize, Deserialize)]
enum DataJournalHeaderData {
    V0(DataJournalHeader),
}

static MAX_HEADER_SIZE: usize = 128;

impl DataJournalHeaderData  {
    fn current(self) -> DataJournalHeader {
        match self {
            DataJournalHeaderData::V0(data) => data,
        }
    }
}

pub struct DataJournal {
    pub journal: tokio::fs::File,
    pub header_size: u64,
    journal_path: PathBuf,
}

pub async fn file_exists(path: &PathBuf) -> Result<bool, Error> {
    match tokio::fs::metadata(path).await {
        Ok(_) => return Ok(true),
        Err(err) => if let std::io::ErrorKind::NotFound = err.kind() {
            return Ok(false)
        } else {
            return Err(Error::from(err))
        },
    };
}


impl DataJournal {

    pub async fn exists(path: &Path, id: u32) -> Result<bool, Error> {
        let path = path.join(id.to_string() + "_data");
        file_exists(&path).await
    }

    pub async fn erase(mut self) -> Result<(), Error> {
        let _ = self.journal.flush().await;
        drop(self.journal);
        tokio::fs::remove_file(self.journal_path).await?;
        Ok(())
    }

    pub async fn open(path: &Path, id: u32) -> Result<Self, Error> {
        let path = path.join(id.to_string() + "_data");
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

            let header_data: DataJournalHeaderData = encoder.deserialize(&data)?;
            let header_size = encoder.serialized_size(&header_data)?;
            let header = header_data.current();
            
            if header.id != id {
                return Err(Error::JournalHeaderIdError(path, header.id, id))
            }

            header_size
        } else {
            let header = DataJournalHeaderData::V0(DataJournalHeader{id});
            let header = encoder.serialize(&header)?;
            journal.write_all(&header).await?;
            info!("New journal size: {} {header:?}", header.len() as u64);
            header.len() as u64
        };

        Ok(Self {journal, header_size, journal_path: path})
    }

    pub async fn append(&mut self, data: &Vec<u8>) -> Result<u64, Error> {
        let index = self.journal.seek(SeekFrom::End(0)).await?;
        self.journal.write_u32_le(data.len() as u32).await?;
        self.journal.write_all(&data).await?;
        Ok(index)
    }

    pub async fn read(&mut self, location: u64) -> Result<Vec<u8>, Error> {
        let index = self.journal.seek(SeekFrom::Start(location)).await?;
        if index != location {
            return Err(Error::ReadPastEnd)
        }
        let size = self.journal.read_u32_le().await? as usize;
        let mut buffer = vec![0; size];
        let result = self.journal.read_exact(&mut buffer).await?;
        if result != size {
            return Err(Error::ReadPastEnd)
        }
        return Ok(buffer)
    }

    pub async fn sync(&mut self) -> Result<(), Error> {
        Ok(self.journal.sync_all().await?)
    }

    // pub async fn sync_task(&mut self, guard: oneshot::Sender<Error>) {
    //     let cursor = self.journal.try_clone().await;
    //     tokio::spawn(async move {
    //         let cursor = match cursor {
    //             Ok(cursor) => cursor,
    //             Err(err) => {
    //                 let _ = guard.send(Error::from(err));
    //                 return;
    //             }
    //         };
    //         if let Err(err) = cursor.sync_all().await {
    //             let _ = guard.send(Error::from(err));
    //         }
    //     });
    // }

    // pub async fn check_last(&mut self, location: u64) -> Result<(), Error> {
    //     self.read(location).await?;
    //     Ok(())
    // }

    pub async fn size(&mut self) -> Result<u64, Error> {
        Ok(self.journal.seek(SeekFrom::End(0)).await?)
    }

    pub async fn truncate(&mut self, size: u64) -> Result<(), Error> {
        Ok(self.journal.set_len(size).await?)
    }
}


#[cfg(test)]
mod test {
    use tempdir::TempDir;
    use super::DataJournal;

    #[tokio::test]
    async fn data_journal() {
        let dir = TempDir::new("data_journal").unwrap();
        let mut journal = DataJournal::open(dir.path(), 1).await.unwrap();

        let data1 = vec![1, 1, 1, 1, 1];
        let mut data2 = vec![];
        for ii in 0..59382 {
            data2.push((ii % 200) as u8);
        }
        let mut data3 = vec![];
        for ii in 0..5982000 {
            data3.push((ii % 200) as u8);
        }

        let key1 = journal.append(&data1).await.unwrap();
        let key2 = journal.append(&data2).await.unwrap();
        let key3 = journal.append(&data3).await.unwrap();
        let key4 = journal.append(&data1).await.unwrap();

        assert_eq!(journal.read(key1).await.unwrap(), data1);
        assert_eq!(journal.read(key2).await.unwrap(), data2);
        assert_eq!(journal.read(key3).await.unwrap(), data3);
        assert_eq!(journal.read(key4).await.unwrap(), data1);
    }

    #[tokio::test]
    async fn open_journal() {

        let data1 = vec![1, 1, 1, 1, 1];
        let mut data2 = vec![];
        for ii in 0..59382 {
            data2.push((ii % 200) as u8);
        }
        let mut data3 = vec![];
        for ii in 0..5982000 {
            data3.push((ii % 200) as u8);
        }

        let dir = TempDir::new("data_journal").unwrap();
        let (key1, key2, key3, key4) = {
            let mut journal = DataJournal::open(dir.path(), 1).await.unwrap();

            let key1 = journal.append(&data1).await.unwrap();
            let key2 = journal.append(&data2).await.unwrap();
            let key3 = journal.append(&data3).await.unwrap();
            let key4 = journal.append(&data1).await.unwrap();
            // journal.sync().await.unwrap();

            (key1, key2, key3, key4)
        };
        {
            let mut journal = DataJournal::open(dir.path(), 1).await.unwrap();
            assert_eq!(journal.read(key1).await.unwrap(), data1);
            assert_eq!(journal.read(key2).await.unwrap(), data2);
            assert_eq!(journal.read(key3).await.unwrap(), data3);
            assert_eq!(journal.read(key4).await.unwrap(), data1);

        }
    }
}

