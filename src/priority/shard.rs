use std::collections::{HashMap, BinaryHeap};
use std::io::{Write, Read};
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use log::{error, debug};
use tokio::sync::{mpsc, oneshot};

use crate::data_journal::DataJournal;
use crate::error::Error;
use crate::operation_journal::OperationJournal;
use crate::priority::entry::Entry;
use crate::session::{SessionClient, NotificationMask};

use super::{ClientMessage, ClientMessageType};
use super::broker::{AssignParams, EntryPrefix, PopParams, QueueCommand, FinishParams};
use super::entry::{EntryChange, EntryHeader, Firmness, Assignment, ClientId, SequenceNo};


pub struct Shard {
    pub id: u32,
    max_priority: i16,
    min_priority: i16,
    inserted_items: u64,
    inserted_bytes: u64,
    connection: mpsc::Sender<ShardCommand>,
    entry_limit: u64,
    size_limit: u64,
}

enum ProcessExitCode {
    Continue,
    Stop,
    Retire
}

impl Shard {

    pub async fn exists(path: &PathBuf, id: u32) -> Result<bool, Error> {
        if OperationJournal::<EntryChange>::exists(path, id).await? {
            return Ok(true)
        }
        if DataJournal::exists(path, id).await? {
            return Ok(true)
        }
        return Ok(false)
    }

    pub async fn open(client: SessionClient, queue_connection: mpsc::Sender<QueueCommand>, path: PathBuf, id: u32, retry_limit: u32, max_bytes: u64, max_entries: u64) -> Result<(Self, Vec<EntryPrefix>), Error> {
        let (connection, recv) = mpsc::channel(32);
        let data_journal = DataJournal::open(&path, id).await?;
        let meta_journal = OperationJournal::open(&path, id).await?;

        let data_cursor = data_journal.header_size;

        let mut internal = ShardInternal {
            id,
            data_journal,
            meta_journal,
            pending_messages: Default::default(),
            syncing_messages: Default::default(),
            sync_task: oneshot::channel().1,
            entries: Default::default(),
            assignments: Default::default(),
            timeouts: Default::default(),
            connection: recv,
            client,
            data_cursor,
            queue_connection,
            entry_limit: max_entries,
            size_limit: max_bytes,
            inserted_entries: 0,
            inserted_bytes: 0,
            retry_limit,
        };

        let raw_operations = internal.meta_journal.read_all().await?;
        let ending = internal.data_journal.size().await?;
        // let mut last_location: Option<u64> = None;
        let mut max_sequence: u64 = 0;
        let mut inserted_items = 0;
        let mut inserted_bytes: u64 = 0;
        // let mut operations = vec![];
        for (location, operation) in raw_operations {
            internal.apply(&operation);
            if let EntryChange::New(entry, encoded_length) = &operation {
                if entry.location + 4 + *encoded_length as u64 > ending {
                    internal.meta_journal.truncate(location).await?;
                    internal.data_journal.truncate(entry.location).await?;
                    break
                }
                inserted_items += 1;
                inserted_bytes += entry.length as u64;
                // last_location = Some(match last_location {
                //     Some(value) => value.max(entry.location),
                //     None => entry.location,
                // });
                max_sequence = max_sequence.max(entry.sequence.0);
            }
            // operations.push(operation);
        }

        // internal.data_journal.truncate()
        // if let Some(location) = last_location {
        //     internal.data_journal.check_last(location).await?;
        // }

        let mut active = vec![];
        let mut max_priority = i16::MIN;
        let mut min_priority = i16::MAX;
        for (_, entry) in internal.entries.iter() {
            active.push(EntryPrefix {
                priority: entry.priority,
                sequence: entry.sequence,
                shard: entry.shard,
            });
            max_priority = max_priority.max(entry.priority);
            min_priority = min_priority.min(entry.priority);
        }

        tokio::spawn(async move {
            if let Err(err) = internal.run().await {
                error!("Shard error: {err:?}");
            }
        });

        let shard = Self {
            id,

            max_priority,
            min_priority,

            connection,
            inserted_items,
            inserted_bytes,
            entry_limit: max_entries,
            size_limit: max_bytes,
        };

        return Ok((shard, active))
    }


    pub fn can_insert(&self) -> bool {
        self.inserted_items < self.entry_limit && self.inserted_bytes < self.size_limit
    }

    pub fn priority_fit(&self, priority: i16) -> i32 {
        if priority > self.max_priority {
            priority as i32 - self.max_priority as i32
        } else if priority < self.min_priority {
            -(self.min_priority as i32 - priority as i32)
        } else {
            0
        }
    }

    pub async fn insert(&mut self, entry: Entry) -> Result<(), Error> {
        self.min_priority = self.min_priority.min(entry.header.priority);
        self.max_priority = self.max_priority.max(entry.header.priority);
        self.inserted_items += 1;
        self.inserted_bytes += entry.body.len() as u64;
        Ok(self.connection.send(ShardCommand::Insert(entry)).await?)
    }

    pub async fn assign(&self, assign: AssignParams, entry: EntryPrefix) -> Result<(), Error> {
        Ok(self.connection.send(ShardCommand::Assign(assign, entry)).await?)
    }

    pub async fn finish(&self, finish: FinishParams) -> Result<(), Error> {
        Ok(self.connection.send(ShardCommand::Finish(finish)).await?)
    }

    pub async fn pop(&self, pop: PopParams, entry: EntryPrefix) -> Result<(), Error> {
        Ok(self.connection.send(ShardCommand::Pop(pop, entry)).await?)
    }

    pub async fn stop(&self) -> Result<(), Error> {
        let (send, recv) = oneshot::channel();
        self.connection.send(ShardCommand::Stop(send)).await?;
        let _ = recv.await;
        return Ok(())
    }

    pub async fn exhaust_check(&self) -> Result<(), Error> {
        Ok(self.connection.send(ShardCommand::ExhaustCheck).await?)
    }

    pub async fn retire(&self) -> Result<(), Error> {
        Ok(self.connection.send(ShardCommand::Retire).await?)
    }

}

enum ShardCommand {
    Insert(Entry),
    Assign(AssignParams, EntryPrefix),
    Finish(FinishParams),
    Pop(PopParams, EntryPrefix),
    Stop(oneshot::Sender<()>),
    ExhaustCheck,
    Retire,
}

#[derive(PartialEq, Eq)]
struct TimeoutSlot {
    pub expiry: chrono::DateTime<chrono::Utc>,
    pub client: ClientId,
    pub sequence: SequenceNo
}

impl Ord for TimeoutSlot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expiry.cmp(&other.expiry)
        // other.expiry.cmp(&self.expiry)
    }
}

impl PartialOrd for TimeoutSlot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.cmp(&self))
    }
}

struct ShardInternal {
    pub id: u32,
    data_journal: DataJournal,
    meta_journal: OperationJournal<EntryChange>,
    pending_messages: Vec<ClientMessage>,
    syncing_messages: Vec<ClientMessage>,
    sync_task: oneshot::Receiver<Error>,
    data_cursor: u64,

    inserted_entries: u64,
    inserted_bytes: u64,
    entry_limit: u64,
    size_limit: u64,

    entries: HashMap<SequenceNo, EntryHeader>,
    assignments: HashMap<SequenceNo, (EntryHeader, Assignment)>,
    timeouts: BinaryHeap<TimeoutSlot>,
    retry_limit: u32,

    queue_connection: mpsc::Sender<QueueCommand>,
    connection: mpsc::Receiver<ShardCommand>,
    client: SessionClient,
}

unsafe impl Send for ShardInternal {}

impl ShardInternal {
    async fn run(mut self) -> Result<(), Error> {
        loop {
            let mut next_timeout = self.process_timeouts().await;

            if self.pending_messages.len() > 0 || self.syncing_messages.len() > 0 {
                if !self.sync_task_running().await {
                    self.cycle_sync().await;
                    debug!("Timer based message sync");
                } else {
                    next_timeout = tokio::time::Instant::now().add(tokio::time::Duration::from_millis(250));
                }
            }

            tokio::select!{
                message = self.connection.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => return Ok(()),
                    };
        
                    match self.process_message(message).await {
                        Ok(code) => match code {
                            ProcessExitCode::Continue => continue,
                            ProcessExitCode::Stop => break,
                            ProcessExitCode::Retire => {
                                return Ok(self.retire())
                            },
                        },
                        Err(err) => error!("Shard Error: {err:?}")
                    };        
                }
                _ = tokio::time::sleep_until(next_timeout) => {}
            }
        }
        return Ok(())
    }

    async fn process_message(&mut self, command: ShardCommand) -> Result<ProcessExitCode, Error> {
        match command {
            ShardCommand::Stop(response) => {
                let _ = self.data_journal.sync().await;
                let _ = self.meta_journal.sync().await;
                let _ = response.send(());
                return Ok(ProcessExitCode::Stop)
            }
            ShardCommand::ExhaustCheck => {
                if self.is_exhausted() {
                    let _ = self.queue_connection.send(QueueCommand::ShardFinished(self.id)).await;
                }
                return Ok(ProcessExitCode::Continue)
            }
            ShardCommand::Insert(entry) => self.do_insert(entry).await,
            ShardCommand::Assign(params, entry) => self.do_assign(params, entry).await,
            ShardCommand::Finish(params) => self.do_finish(params).await,
            ShardCommand::Pop(pop, entry) => self.do_pop(pop, entry).await,
            ShardCommand::Retire => {
                return Ok(ProcessExitCode::Retire)
            },
        }?;
        return Ok(ProcessExitCode::Continue)
    }

    async fn do_reinsert(&mut self, sequence: SequenceNo, priority: i16, assignment: Assignment) -> Result<(), Error> {
        let change = EntryChange::AssignTimeout(assignment.client, sequence, assignment.expiry);
        let result = self.apply(&change);
        self.meta_journal.append(&change).await?;

        debug!("retry message {:?}.", sequence);

        if let Some(result) = result {
            debug!("dropping message {:?} after {} retries.", result.sequence, result.attempts);
            self.sync_events(vec![
                ClientMessage{ client: assignment.notice.client, label: assignment.notice.label, notice: ClientMessageType::Retry},
                ClientMessage{ client: assignment.notice.client, label: assignment.notice.label, notice: ClientMessageType::Drop},
            ]).await;
        } else {
            self.sync_event(ClientMessage{ client: assignment.notice.client, label: assignment.notice.label, notice: ClientMessageType::Retry}).await;
            self.queue_connection.send(QueueCommand::ReInsert(EntryPrefix{ 
                priority, 
                sequence, 
                shard: self.id 
            })).await?;
        }
        return Ok(())
    }

    async fn do_insert(&mut self, entry: Entry) -> Result<(), Error> {
        let Entry {mut header, body} = entry;
        header.location = self.data_cursor;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(self.client.config.compression.into()));
        encoder.write_all(&body)?;        
        let body = encoder.finish()?;

        debug!("Insert at {}", header.location);
        let note = header.notice;
        let change = EntryChange::New(header, body.len() as u32);
        self.apply(&change);

        let (index, meta_write) = tokio::join!(
            self.data_journal.append(&body),
            self.meta_journal.append(&change)
        );
        match index {
            Ok(index) => {
                if let EntryChange::New(entry, _) = change {
                    if entry.location != index {
                        return Err(Error::ShardIndexDesync(entry.location, index))
                    }
                }
            },
            Err(err) => return Err(err),
        }
        if let Err(err) = meta_write {
            return Err(err);
        }

        if note.notices.contains(NotificationMask::Write) {
            self.client.send_client(ClientMessage{
                client: note.client,
                label: note.label,
                notice: ClientMessageType::Write
            }).await?;
        }
        if note.notices.contains(NotificationMask::Sync) {
            self.sync_event(ClientMessage{
                client: note.client,
                label: note.label,
                notice: ClientMessageType::Sync
            }).await;
        }
        return Ok(())
    }

    async fn do_assign(&mut self, params: AssignParams, prefix: EntryPrefix) -> Result<(), Error> {
        let timeout = if params.work_timeout > 0.0 {
            chrono::Duration::milliseconds((1000.0 * params.work_timeout) as i64)
        } else {
            chrono::Duration::days(100)
        };
        let timeout = chrono::Utc::now() + timeout;

        let change = EntryChange::Assign(params.client, prefix.sequence, timeout);

        let header = match self.apply(&change) {
            Some(header) => header,
            None => {
                error!("SHARD DESYNC on assign");
                return Ok(())
            },
        };
        let notice = header.notice;

        let body = self.data_journal.read(header.location).await?;
        let mut decoder = ZlibDecoder::new(&body[..]);
        let mut output = vec![];
        decoder.read_to_end(&mut output)?;

        let entry = Entry{header, body: Arc::new(output)};

        match params.sync {
            Firmness::Ready => {
                self.client.send_client(ClientMessage{ client: params.client, label: params.label, notice: ClientMessageType::SendEntry(entry, false) }).await?;
                self.meta_journal.append(&change).await?;
            },
            Firmness::Write => {
                self.meta_journal.append(&change).await?;
                self.client.send_client(ClientMessage{ client: params.client, label: params.label, notice: ClientMessageType::SendEntry(entry, false) }).await?;
            },
            Firmness::Sync => {
                self.meta_journal.append(&change).await?;
                self.sync_event(ClientMessage{ client: params.client, label: params.label, notice: ClientMessageType::SendEntry(entry, false) }).await;
            },
        }

        if notice.notices.contains(NotificationMask::Assign) {
            self.sync_event(ClientMessage { client: notice.client, label: notice.label, notice: ClientMessageType::Assign }).await;
        };

        return Ok(())
    }

    async fn do_finish(&mut self, params: FinishParams) -> Result<(), Error> {
        let FinishParams{client, sequence, response, label} = params;
        let change = EntryChange::Finish(client, sequence);
        let note = match self.apply(&change) {
            Some(entry) => entry.notice,
            None => {
                error!("SHARD DESYNC on finish");
                return Ok(())
            },
        };

        let (response, label) = match label {
            Some(label) => match response {
                Some(resp) => (Some(resp), label),
                None => (Some(Firmness::Ready), label),
            },
            None => (None, 0),
        };

        match response {
            Some(sync) => match sync {
                Firmness::Ready => {
                    let _ = self.client.send_client(ClientMessage {client, label, notice: ClientMessageType::Finish}).await;
                    self.meta_journal.append(&change).await?;
                },
                Firmness::Write => {
                    self.meta_journal.append(&change).await?;
                    let _ = self.client.send_client(ClientMessage {client, label, notice: ClientMessageType::Finish}).await;
                },
                Firmness::Sync => {
                    self.meta_journal.append(&change).await?;
                    self.sync_event(ClientMessage {client, label, notice: ClientMessageType::Finish}).await;
                },
            },
            None => self.meta_journal.append(&change).await?,
        }
        if note.notices.contains(NotificationMask::Finish) {
            self.sync_event(ClientMessage {
                client: note.client,
                label: note.label,
                notice: ClientMessageType::Finish
            }).await;
        };
        if self.is_exhausted() {
            let _ = self.queue_connection.send(QueueCommand::ShardFinished(self.id)).await;
        }
        return Ok(())
    }

    async fn do_pop(&mut self, pop: PopParams, prefix: EntryPrefix) -> Result<(), Error> {
        let change = EntryChange::Pop(prefix.sequence);
        let header = match self.apply(&change) {
            Some(header) => header,
            None => {
                error!("SHARD DESYNC on pop");
                return Ok(())
            },
        }.clone();
        let notice = header.notice;

        let body = self.data_journal.read(header.location).await?;
        let mut decoder = ZlibDecoder::new(&body[..]);
        let mut output = vec![];
        decoder.read_to_end(&mut output)?;

        let entry = Entry{header, body: Arc::new(output)};

        match pop.sync {
            Firmness::Ready => {
                self.client.send_client(ClientMessage{ client: pop.client, label: pop.label, notice: ClientMessageType::SendEntry(entry, true) }).await?;
                self.meta_journal.append(&change).await?;
            },
            Firmness::Write => {
                self.meta_journal.append(&change).await?;
                self.client.send_client(ClientMessage{ client: pop.client, label: pop.label, notice: ClientMessageType::SendEntry(entry, true) }).await?;
            },
            Firmness::Sync => {
                self.meta_journal.append(&change).await?;
                self.sync_event(ClientMessage{ client: pop.client, label: pop.label, notice: ClientMessageType::SendEntry(entry, true) }).await;
            }
        }

        if notice.notices.contains(NotificationMask::Finish) {
            debug!("Pop finish notice for {} {}", notice.client, notice.label);
            self.sync_event(ClientMessage { client: notice.client, label: notice.label, notice: ClientMessageType::Finish }).await;
        } else {
            debug!("Pop finish no notice");
        }

        return Ok(())
    }

    fn apply(&mut self, op: &EntryChange) -> Option<EntryHeader> {
        match op {
            EntryChange::New(entry, encoded_length) => {
                self.data_cursor += *encoded_length as u64 + 4;
                self.inserted_entries += 1;
                self.inserted_bytes += entry.length as u64;
                self.entries.insert(entry.sequence, entry.clone());
                None
            },
            EntryChange::Assign(client, sequence, timeout) => {
                let mut entry = match self.entries.remove(sequence) {
                    Some(entry) => entry,
                    None => return None,
                };
                entry.attempts += 1;
                self.assignments.insert(*sequence, (
                    entry.clone(),
                    Assignment{ expiry: *timeout, client: *client, notice: entry.notice }
                ));
                self.timeouts.push(TimeoutSlot{
                    expiry: *timeout,
                    client: *client,
                    sequence: *sequence,
                });
                Some(entry)
            },
            EntryChange::Finish(client, sequence) => {
                match self.assignments.entry(*sequence) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let ok = {
                            let (_, assignment) = entry.get();
                            assignment.client == *client
                        };
                        if ok {
                            Some(entry.remove().0)
                        } else {
                            None
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(_) => None,
                }
            },
            EntryChange::Pop(sequence) => {
                match self.entries.remove(sequence) {
                    Some(value) => Some(value),
                    None => None,
                }
            },
            EntryChange::AssignTimeout(client, sequence, timeout) => {
                let entry = match self.assignments.entry(*sequence) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let (_, assignment) = entry.get();
                        if assignment.client == *client && *timeout == assignment.expiry {
                            Some(entry.remove())
                        } else {
                            None
                        }
                    },
                    std::collections::hash_map::Entry::Vacant(_) => None,
                };

                if let Some((entry, _)) = entry {
                    if entry.attempts >= self.retry_limit {
                        return Some(entry);
                    } else {
                        self.entries.insert(entry.sequence, entry);
                    }
                }

                None
            },
        }
    }

    async fn sync_task_running(&mut self) -> bool {
        match self.sync_task.try_recv() {
            Ok(err) => {
                error!("{err:?}");
                // Send error notices here
                false
            },
            Err(err) => match err {
                oneshot::error::TryRecvError::Empty => true,
                oneshot::error::TryRecvError::Closed => false,
            },
        }
    }

    async fn cycle_sync(&mut self) {
        loop {
            match self.syncing_messages.pop() {
                Some(message) => {let _ = self.client.send_client(message).await;},
                None => break,
            }
        }
        std::mem::swap(&mut self.syncing_messages, &mut self.pending_messages);
        let (guard, recv) = oneshot::channel();
        self.sync_task = recv;

        let m = match self.meta_journal.journal.try_clone().await {
            Ok(m) => m,
            Err(err) => {
                error!("{err:?}");
                return
            },
        };
        let j = match self.data_journal.journal.try_clone().await {
            Ok(j) => j,
            Err(err) => {
                error!("{err:?}");
                return
            },
        };

        debug!("Start sync job with {} messages", self.syncing_messages.len());
        tokio::spawn(async move {
            let (m, j) = tokio::join!(m.sync_data(), j.sync_data());
            if let Err(err) = m {
                let _ = guard.send(Error::from(err));
                return
            }
            if let Err(err) = j {
                let _ = guard.send(Error::from(err));
            }
        });
    }

    async fn sync_events(&mut self, mut events: Vec<ClientMessage>) {
        let last = events.pop();
        for item in events {
            self.pending_messages.push(item);
        }
        if let Some(last) = last {
            self.sync_event(last).await;
        }
    }

    async fn sync_event(&mut self, event: ClientMessage) {
        self.pending_messages.push(event);
        if !self.sync_task_running().await {
            self.cycle_sync().await
        } else {
            debug!("Sync message buffered.");
        }
    }

    fn retire(mut self) {
        tokio::spawn(async move {
            if let Ok(err) = self.sync_task.await {
                error!("{err}");
            }
            if self.pending_messages.len() > 0 {
                let _ = self.meta_journal.sync().await;
                for message in self.pending_messages {
                    let _ = self.client.send_client(message).await;
                }
            }
            
            let (a, b) = tokio::join!(
                self.data_journal.erase(),
                self.meta_journal.erase()
            );
            if let Err(err) = a {
                error!("{err}");
            }
            if let Err(err) = b {
                error!("{err}");
            }
        });
    }

    async fn process_timeouts(&mut self) -> tokio::time::Instant {
        loop {
            {
                let exp = match self.timeouts.peek() {
                    Some(next) => next,
                    None => break,
                }.expiry;
                if exp > chrono::Utc::now() { break }
            }

            let next = match self.timeouts.pop() {
                Some(next) => next,
                None => break,
            };

            let row = match self.assignments.entry(next.sequence) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let (header, assignment) = entry.get();
                    if assignment.client == next.client && next.expiry == assignment.expiry {
                        Some((header.sequence, header.priority, assignment.clone()))
                    } else {
                        None
                    }
                },
                std::collections::hash_map::Entry::Vacant(_) => continue,
            };

            if let Some((sequence, priority, assignment)) = row {
                if let Err(err) = self.do_reinsert(sequence, priority, assignment).await {
                    error!("Error in timeout reinsert: {err}");
                }
            }
        }

        // info!("timeouts left {}", self.timeouts.len());
        match self.timeouts.peek() {
            Some(next) => {
                // next.expiry.
                // tokio::time::Instant::from(next.expiry);
                // info!("timeout in {:?}", (next.expiry - chrono::Utc::now()).to_std().unwrap_or(tokio::time::Duration::from_secs(60)));
                tokio::time::Instant::now().add(
                    (next.expiry - chrono::Utc::now()).to_std().unwrap_or(tokio::time::Duration::from_secs(60))
                )
            },
            None => tokio::time::Instant::now().add(tokio::time::Duration::from_secs(60)),
        }
    }

    pub fn can_insert(&self) -> bool {
        self.inserted_entries < self.entry_limit && self.inserted_bytes < self.size_limit
    }

    pub fn is_exhausted(&self) -> bool {
        !self.can_insert() && self.entries.len() == 0 && self.assignments.len() == 0
    }
}