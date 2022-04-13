use std::collections::{HashMap, BTreeSet, VecDeque};
use std::path::PathBuf;

use futures::future::join_all;
use log::{info, error, debug};
use rand::Rng;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

use crate::config::QueueConfiguration;
use crate::error::Error;
use crate::session::{SessionClient, NotificationMask, QueueStatusResponse};
use super::entry::{Entry, Firmness, ClientId, SequenceNo};
use super::{ClientMessage, ClientMessageType};
use super::shard::Shard;

pub struct AssignParams {
    pub blocking: bool,
    pub client: ClientId,
    pub label: u64,
    pub block_timeout: f32,
    pub work_timeout: f32,
    pub sync: Firmness
}

pub struct PopParams {
    pub blocking: bool,
    pub client: ClientId,
    pub label: u64,
    pub block_timeout: f32,
    pub sync: Firmness
}


pub struct FinishParams {
    pub client: ClientId, 
    pub sequence: SequenceNo,
    pub label: Option<u64>,
    pub response: Option<Firmness>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct QueueStatus {
    pub length: u64,
    pub readers_waiting: u64,
    pub shards_read_write: u32,
    pub shards_read_only: u32,
    pub shards_finished: u64,
}

pub enum QueueCommand {
    Insert(Entry),
    Assign(AssignParams),
    Finish(FinishParams, u32),
    Pop(PopParams),
    ReInsert(EntryPrefix),
    Stop(oneshot::Sender<()>),
    Status(oneshot::Sender<QueueStatusResponse>),
    ShardFinished(u32)
}

type PriorityQueue = mpsc::Sender<QueueCommand>;

#[derive(Serialize, Deserialize)]
struct QueueHeaderFile {
    name: String,
    retries: u32,
    shard_max_bytes: u64,
    shard_max_records: u64,
}

#[derive(Serialize, Deserialize)]
enum QueueHeaderFileData {
    V0(QueueHeaderFile)
}

impl QueueHeaderFileData {
    fn current(self) -> QueueHeaderFile {
        match self {
            QueueHeaderFileData::V0(data) => data,
        }
    }
}

pub async fn create_queue(path: &PathBuf, client: SessionClient, config: QueueConfiguration) -> Result<(String, PriorityQueue), Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let path = path.join(&id);
    tokio::fs::DirBuilder::new()
        .recursive(true)
        .create(&path)
        .await?;
    info!("Creating queue: {} for {id}", config.name);
    tokio::fs::write(path.join("header"), bincode::serialize(&QueueHeaderFileData::V0(QueueHeaderFile {
        name: config.name,
        retries: config.max_retries,
        shard_max_bytes: config.shard_max_bytes,
        shard_max_records: config.shard_max_records
    }))?).await?;
    return open_queue(path, client).await
}

pub async fn open_queue(path: PathBuf, client: SessionClient) -> Result<(String, PriorityQueue), Error> {
    let (command_sink, command_source) = mpsc::channel(128);

    let header = tokio::fs::read(path.join("header")).await?;
    let header: QueueHeaderFileData = bincode::deserialize(&header)?;
    let header = header.current();

    let internal = PriorityQueueInternal {
        sequence_counter: 0,
        rw_shards: Default::default(),
        ro_shards: Default::default(),
        path,
        session: client,
        available: Default::default(),
        command_sink: command_sink.clone(),
        commands: command_source,
        pending_requests: Default::default(),
        shards_finished: 0,
        retry_limit: header.retries,
        shard_max_bytes: header.shard_max_bytes,
        shard_max_records: header.shard_max_records,
    };
    tokio::spawn(internal.run());

    return Ok((header.name, command_sink))
}


#[derive(PartialEq, Eq, Clone)]
pub struct EntryPrefix {
    pub priority: u32,
    pub sequence: SequenceNo,
    pub shard: u32,
}

impl Ord for EntryPrefix {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.priority.cmp(&other.priority).then_with(|| other.sequence.cmp(&self.sequence)) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        }
    }
}

impl PartialOrd for EntryPrefix {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

enum PendingRequest {
    Pop(PopParams, std::time::Instant),
    Assign(AssignParams, std::time::Instant)
}

struct PriorityQueueInternal {
    session: SessionClient,
    rw_shards: HashMap<u32, Shard>,
    ro_shards: HashMap<u32, Shard>,
    available: BTreeSet<EntryPrefix>,
    command_sink: mpsc::Sender<QueueCommand>,
    commands: mpsc::Receiver<QueueCommand>,
    pending_requests: VecDeque<PendingRequest>,
    sequence_counter: u64,
    shards_finished: u64,
    path: PathBuf,
    retry_limit: u32,
    shard_max_bytes: u64,
    shard_max_records: u64,
}

impl PriorityQueueInternal {
    async fn load_shards(&mut self) -> Result<(), Error> {
        let mut dir_listing = tokio::fs::read_dir(&self.path).await?;
        let mut loading = vec![];
        while let Some(entry) = dir_listing.next_entry().await? {
            if !entry.file_type().await?.is_file() { continue }
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            let parts: Vec<&str> = name.split("_").collect();
            if parts.len() == 2 && parts[1] == "meta" {
                if let Ok(id) = parts[0].parse::<u32>() {
                    loading.push(Shard::open(
                        self.session.clone(), 
                        self.command_sink.clone(),
                        self.path.clone(), 
                        id, 
                        self.retry_limit,
                        self.shard_max_bytes,
                        self.shard_max_records,
                    ));
                }
            }
        }

        for res in join_all(loading).await {
            match res {
                Ok((shard, existing)) => {
                    for entry in existing {
                        self.sequence_counter = self.sequence_counter.max(entry.sequence.0) + 1;
                        self.available.insert(entry);
                    }
                    if shard.can_insert() {
                        self.rw_shards.insert(shard.id, shard);
                    } else {
                        self.ro_shards.insert(shard.id, shard);
                    }
                },
                Err(err) => { error!("{err:?}"); continue },
            };
        }

        return Ok(());
    }

    async fn run(mut self) {
        if let Err(err) = self.load_shards().await {
            error!("Error loading shards {err:?}");
        }

        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(60 * 5));
        cleanup_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                message = self.commands.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => return,
                    };
        
                    match self.process_message(message).await {
                        Ok(do_break) => if do_break { break },
                        Err(err) => error!("Broker Error: {err:?}")
                    };        
                }

                _ = cleanup_interval.tick() => {
                    if let Err(err) = self.cleanup().await {
                        error!("Cleanup error: {err}");
                    }
                }
            }
        }
    }

    async fn process_message(&mut self, message: QueueCommand) -> Result<bool, Error> {
        match message {
            QueueCommand::Stop(signal) => {
                let mut out = vec![];
                for (_, shard) in self.ro_shards.iter().chain(self.rw_shards.iter()) {
                    out.push(shard.stop());
                }

                for result in join_all(out).await{
                    if let Err(err) = result {
                        error!("Shard stop error {err}");
                    }
                }

                let _ = signal.send(());
                return Ok(true)
            },
            QueueCommand::ShardFinished(id) => self.retire_shard(id).await,
            QueueCommand::Insert(insert) => self.do_insert(insert).await,
            QueueCommand::Assign(assign) => self.do_assign(assign).await,
            QueueCommand::Finish(params, shard ) => self.do_finish(shard, params).await,
            QueueCommand::Pop(pop) => self.do_pop(pop).await,
            QueueCommand::ReInsert(reinsert) => self.do_reinsert(reinsert).await,
            QueueCommand::Status(result) => self.do_status(result).await,
        }?;
        return Ok(false)
    }
    
    async fn do_insert(&mut self, mut entry: Entry) -> Result<(), Error> {
        // Assign a sequence number to entry
        entry.header.sequence = SequenceNo(self.sequence_counter);
        self.sequence_counter += 1;
        let mut assignment = None;

        // See if there is a pending request to process
        while let Some(request) = self.pending_requests.pop_front() {
            match request {
                PendingRequest::Pop(pop, start) => {
                    if start.elapsed().as_secs_f32() > pop.block_timeout { 
                        self.failed_fetch(pop.client, pop.label).await;
                        continue 
                    }
                    entry = if let Some(entry) = self.do_direct_pop(&pop, entry).await? {
                        entry
                    } else {
                        return Ok(())
                    };
                },
                PendingRequest::Assign(assign, start) => {
                    if start.elapsed().as_secs_f32() > assign.block_timeout { 
                        self.failed_fetch(assign.client, assign.label).await;
                        continue 
                    }
                    assignment = Some(assign);
                    break;
                },
            }
        }

        // No one already wants this message, select a shard
        let mut best: Option<&mut Shard> = None;
        let mut expand: i32 = i32::MAX;
        let mut finished_writing = vec![];

        for (_, shard) in self.rw_shards.iter_mut() {
            if !shard.can_insert() { finished_writing.push(shard.id); continue }
            let gap = shard.priority_fit(entry.header.priority).abs();
            if gap == 0 {
                best = Some(shard);
                break;
            } else if gap < expand {
                expand = gap;
                best = Some(shard);
            }
        }

        // make sure we have a shard to work with
        let shard = match best {
            Some(shard) => shard,
            None => self.create_shard().await?,
        };
        entry.header.shard = shard.id;

        // Send the entry off to be committed to disk
        let notice = entry.header.notice;
        let priority = entry.header.priority;
        let sequence = entry.header.sequence;
        shard.insert(entry).await?;

        // Place the prefix either in the table, or assign
        let prefix = EntryPrefix {
            priority,
            sequence,
            shard: shard.id,
        };
        match assignment {
            Some(assignment) => { shard.assign(assignment, prefix).await?; },
            None => { self.available.insert(prefix); }
        };

        // If the sender wants early confirmation send them that
        if notice.notices.contains(NotificationMask::Ready) {
            self.session.send_client(ClientMessage {
                client: notice.client,
                label: notice.label,
                notice: ClientMessageType::Ready
            }).await?;
        }

        // Clean up shards we can't write to
        for id in finished_writing {
            if let Some(shard) = self.rw_shards.remove(&id) {
                self.ro_shards.insert(id, shard);
            }
        }
        return Ok(())
    }

    async fn do_reinsert(&mut self, reinsert: EntryPrefix) -> Result<(), Error> {
        // See if there is a pending request to process
        while let Some(request) = self.pending_requests.pop_front() {
            match request {
                PendingRequest::Pop(pop, start) => {
                    if start.elapsed().as_secs_f32() > pop.block_timeout { 
                        self.failed_fetch(pop.client, pop.label).await;
                        continue 
                    }
                    if let Some(shard) = self.get_shard(reinsert.shard) {
                        shard.pop(pop, reinsert).await?;
                    }
                    return Ok(())
                },
                PendingRequest::Assign(assign, start) => {
                    if start.elapsed().as_secs_f32() > assign.block_timeout { 
                        self.failed_fetch(assign.client, assign.label).await;
                        continue 
                    }
                    if let Some(shard) = self.get_shard(reinsert.shard) {
                        shard.assign(assign, reinsert).await?;
                    }
                    return Ok(());
                },
            }
        }

        self.available.insert(reinsert);
        return Ok(())
    }

    async fn do_status(&mut self, response: oneshot::Sender<QueueStatusResponse>) -> Result<(), Error> {
        let _ = response.send(QueueStatusResponse::Status(QueueStatus{
            length: self.available.len() as u64,
            readers_waiting: self.pending_requests.len() as u64,
            shards_read_write: self.rw_shards.len() as u32,
            shards_read_only: self.ro_shards.len() as u32,
            shards_finished: self.shards_finished,
        }));
        return Ok(())
    }

    fn pop_first(&mut self) -> Option<EntryPrefix> {
        let entry = match self.available.iter().next() {
            Some(entry) => {
                let entry = entry.clone();
                Some(entry)
            },
            None => None
        };
        match entry {
            Some(entry) => if self.available.remove(&entry) {
                Some(entry)
            } else {
                None
            },
            None => None,
        }
    }

    async fn do_assign(&mut self, assign: AssignParams) -> Result<(), Error> {
        loop {
            match self.pop_first() {
                Some(entry) => {
                    // we have an available entry, take it and move to next stage
                    let shard = match self.get_shard(entry.shard) {
                        Some(shard) => shard,
                        None => continue,
                    };
                    shard.assign(assign, entry).await?;
                    break;
                },
                None => {
                    // No available entry, enqueue this request
                    if assign.blocking {
                        self.pending_requests.push_back(PendingRequest::Assign(assign, std::time::Instant::now()));
                    } else {
                        self.failed_fetch(assign.client, assign.label).await;
                    }
                    break;
                },
            };
        }
        return Ok(())
    }

    async fn do_finish(&mut self, shard: u32, params: FinishParams) -> Result<(), Error> {
        match self.get_shard(shard) {
            Some(shard) => {
                shard.finish(params).await?;
                Ok(())
            },
            None => Ok(()),
        }
    }

    async fn do_pop(&mut self, pop: PopParams) -> Result<(), Error> {
        loop {
            match self.pop_first() {
                Some(entry) => {
                    // we have an available entry, take it and move to next stage
                    let shard = match self.get_shard(entry.shard) {
                        Some(shard) => shard,
                        None => continue,
                    };
                    shard.pop(pop, entry).await?;
                    break;
                },
                None => {
                    if pop.blocking {
                        // No available entry, enqueue this request
                        self.pending_requests.push_back(PendingRequest::Pop(pop, std::time::Instant::now()));
                    } else {
                        self.failed_fetch(pop.client, pop.label).await;
                    }
                    break;
                },
            };
        }
        return Ok(())
    }

    async fn do_direct_pop(&mut self, pop: &PopParams, entry: Entry) -> Result<Option<Entry>, Error> {
        let notice = entry.header.notice;
        debug!("Direct pop {}", notice.label);

        // Try to send the client the message
        let sent = self.session.send_client(ClientMessage {
            client: pop.client,
            label: pop.label,
            notice: ClientMessageType::SendEntry(entry, true),
        }).await?;

        // If sending it failed, send it back
        if let Some(message) = sent {
            if let ClientMessageType::SendEntry(entry, _) = message.notice {
                return Ok(Some(entry))
            }
        } 

        // It was sent OK, notify client
        if notice.notices.contains(NotificationMask::Finish) || notice.notices.contains(NotificationMask::Ready) {
            debug!("Direct pop {} notification", notice.label);
            let _ = self.session.send_client(ClientMessage{
                client: notice.client,
                label: notice.label,
                notice: ClientMessageType::Finish,
            }).await;

        }

        return Ok(None)
    }

    fn get_shard<'a>(&'a self, id: u32) -> Option<&'a Shard> {
        match self.rw_shards.get(&id) {
            Some(value) => Some(value),
            None => self.ro_shards.get(&id),
        }
    }

    async fn create_shard<'a>(&'a mut self) -> Result<&'a mut Shard, Error> {
        loop {
            let id: u32 = rand::thread_rng().gen();

            if self.ro_shards.contains_key(&id) { continue }
            if self.rw_shards.contains_key(&id) { continue }
            if Shard::exists(&self.path, id).await? {
                continue;
            }

            let (shard, _) = Shard::open(
                self.session.clone(), 
                self.command_sink.clone(), 
                self.path.clone(), 
                id, 
                self.retry_limit,
                self.shard_max_bytes,
                self.shard_max_records
            ).await?;

            self.rw_shards.insert(id, shard);
            return Ok(self.rw_shards.get_mut(&id).unwrap())
        }
    }

    async fn failed_fetch(&mut self, client: ClientId, label: u64) {
        let _ = self.session.send_client(ClientMessage{
            client,
            label,
            notice: ClientMessageType::NoEntry
        }).await;
    }

    async fn cleanup(&self) -> Result<(), Error> {
        for (_, shard) in self.ro_shards.iter() {
            shard.exhaust_check().await?
        }
        return Ok(())
    }

    async fn retire_shard(&mut self, id: u32) -> Result<(), Error> {
        if let Some(shard) = self.ro_shards.remove(&id) {
            self.shards_finished += 1;
            shard.retire().await?;
        };
        return Ok(())
    }

}
