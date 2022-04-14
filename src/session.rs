use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// use flate2::Compression;
// use flate2::read::ZlibDecoder;
// use flate2::write::ZlibEncoder;
use futures::{StreamExt, SinkExt};
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::{HyperWebsocket, WebSocketStream};
use log::{error, info, debug};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::data_journal::file_exists;
use crate::error::Error;
use crate::priority::broker::{QueueCommand, AssignParams, PopParams, open_queue, create_queue, FinishParams, QueueStatus};
use crate::priority::{ClientMessageType, ClientMessage};
use crate::priority::entry::{Firmness, Entry, EntryHeader, NoticeRequest, ClientId, SequenceNo};
use crate::server::run_api;
use crate::config::{Configuration, QueueConfiguration};


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

#[bitmask_enum::bitmask(u8)]
pub enum NotificationMask {
    Ready,
    Write,
    Sync,
    Assign,
    Finish,
    Retry,
    Drop,
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

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientPost {
    pub queue: String,
    pub message: Vec<u8>,
    pub priority: u32,
    pub label: u64,
    pub notify: Vec<NotificationName>,
}

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
#[serde(tag="type")]
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


#[derive(Serialize, Deserialize, Debug)]
pub struct ClientDelivery {
    pub label: u64,
    pub shard: u32,
    pub sequence: u64,
    pub body: Vec<u8>,
    pub finished: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientNotice {
    pub label: u64,
    pub notice: NotificationName
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientHello {
    pub id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientNoMessage {
    pub label: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum ErrorCode {
    NoObject,
    ObjectAlreadyExists,
    PermissionDenied
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientError {
    pub label: u64,
    pub code: ErrorCode,
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag="type")]
pub enum ClientResponse {
    Message(ClientDelivery),
    Notice(ClientNotice),
    Hello(ClientHello),
    NoMessage(ClientNoMessage),
    Error(ClientError),
}

impl TryFrom<ClientMessage> for ClientResponse {
    type Error = Error;

    // <ClientResponse as TryFrom<ClientMessage>>::
    fn try_from(note: ClientMessage) -> Result<ClientResponse, Error> {
        Ok(match note.notice {
            ClientMessageType::Ready => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Ready }),
            ClientMessageType::Write => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Write }),
            ClientMessageType::Sync => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Sync }),
            ClientMessageType::Assign => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Assign }),
            ClientMessageType::Finish => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Finish }),
            ClientMessageType::Retry => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Retry }),
            ClientMessageType::Drop => ClientResponse::Notice(ClientNotice{ label: note.label, notice: NotificationName::Drop }),
            ClientMessageType::SendEntry(entry, finished) => {
                let body = match std::sync::Arc::try_unwrap(entry.body) {
                    Ok(body) => body,
                    Err(err) => err.as_ref().clone(),
                };
                // let mut decoder = ZlibDecoder::new(&entry.body[..]);
                // let mut output = vec![];
                // decoder.read_to_end(&mut output)?;

                ClientResponse::Message(ClientDelivery {
                    label: note.label,
                    shard: entry.header.shard,
                    sequence: entry.header.sequence.0,
                    body: body,
                    finished,
                })
            },
            ClientMessageType::NoEntry => ClientResponse::NoMessage(ClientNoMessage{
                label: note.label,
            })
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum QueueStatusErrorKind {
    QueueNotFound
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueStatusError {
    name: String,
    code: QueueStatusErrorKind,
    message: String
}

impl QueueStatusError {
    pub fn new(name: String, kind: QueueStatusErrorKind) -> Self {
        Self {
            name,
            code: kind,
            message: match kind {
                QueueStatusErrorKind::QueueNotFound => "Named queue not found".to_owned(),
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag="type")]
pub enum QueueStatusResponse {
    Error(QueueStatusError),
    Status(QueueStatus)
}

enum SessionMessage {
    Connection(HyperWebsocket, Option<ClientId>, bool),
    GetQueue(String, oneshot::Sender<mpsc::Sender<QueueCommand>>),
    GetConnection(ClientId, oneshot::Sender<mpsc::Sender<SocketMessage>>),
    Stop,
    GetQueueStatus(String, oneshot::Sender<QueueStatusResponse>),
    CreateQueue(ClientId, ClientCreate)
}

#[derive(Clone)]
pub struct SessionClient {
    pub config: Arc<Configuration>,
    connection: mpsc::Sender<SessionMessage>,
    queue_cache: HashMap<String, mpsc::Sender<QueueCommand>>,
    client_cache: HashMap<ClientId, mpsc::Sender<SocketMessage>>,
}

impl SessionClient {
    pub async fn serve_websocket(&self, socket: HyperWebsocket, client_id: Option<ClientId>, durable_session: bool) -> Result<(), Error> {
        Ok(self.connection.send(SessionMessage::Connection(socket, client_id, durable_session)).await?)
    }

    pub async fn stop(&self) -> Result<(), Error> {
        self.connection.send(SessionMessage::Stop).await?;
        Ok(self.connection.closed().await)
    }

    async fn fetch_queue<'a>(&'a mut self, name: &String) -> Result<&'a mpsc::Sender<QueueCommand>, Error> {
        if !self.queue_cache.contains_key(name) {
            let (send, recv) = oneshot::channel();
            self.connection.send(SessionMessage::GetQueue(name.to_owned(), send)).await?;
            self.queue_cache.insert(name.to_owned(), recv.await?);
        }
        return Ok(self.queue_cache.get(name).unwrap())
    }

    async fn fetch_client<'a>(&'a mut self, id: ClientId) -> Result<&'a mpsc::Sender<SocketMessage>, Error> {
        if !self.client_cache.contains_key(&id) {
            let (send, recv) = oneshot::channel();
            self.connection.send(SessionMessage::GetConnection(id, send)).await?;
            self.client_cache.insert(id, recv.await?);
        }
        return Ok(self.client_cache.get(&id).unwrap())
    }

    pub async fn post(&mut self, client: ClientId, post: ClientPost) -> Result<(), Error> {
        let queue = self.fetch_queue(&post.queue.to_owned()).await?;
        Ok(queue.send(QueueCommand::Insert(Entry {
            header: EntryHeader {
                shard: 0,
                length: post.message.len() as u32,
                location: 0,
                attempts: 0,
                priority: post.priority,
                sequence: SequenceNo(0),
                notice: NoticeRequest {
                    label: post.label,
                    client,
                    notices: NotificationMask::from(&post.notify),
                },
            },
            body: Arc::new(post.message),
        })).await?)
    }

    pub async fn fetch(&mut self, client: ClientId, post: ClientFetch) -> Result<(), Error> {
        let queue = self.fetch_queue(&post.queue).await?;
        Ok(queue.send(QueueCommand::Assign(AssignParams{
            blocking: post.blocking,
            client,
            label: post.label,
            work_timeout: post.work_timeout,
            block_timeout: post.block_timeout,
            sync: post.sync,
        })).await?)
    }

    pub async fn finish(&mut self, client: ClientId, finish: ClientFinish) -> Result<(), Error> {
        let queue = self.fetch_queue(&finish.queue.to_owned()).await?;
        Ok(queue.send(QueueCommand::Finish(FinishParams{
            client,
            sequence: SequenceNo(finish.sequence),
            label: finish.label,
            response: finish.response,
        }, finish.shard)).await?)
    }

    pub async fn pop(&mut self, client: ClientId, pop: ClientPop) -> Result<(), Error> {
        let queue = self.fetch_queue(&pop.queue.to_owned()).await?;
        Ok(queue.send(QueueCommand::Pop(PopParams{
            blocking: pop.blocking,
            client,
            label: pop.label,
            block_timeout: pop.block_timeout,
            sync: pop.sync,
        })).await?)
    }

    pub async fn create(&mut self, client: ClientId, pop: ClientCreate) -> Result<(), Error> {
        self.connection.send(SessionMessage::CreateQueue(client, pop)).await?;
        return Ok(())
    }

    pub async fn send_client(&mut self, message: crate::priority::ClientMessage) -> Result<Option<crate::priority::ClientMessage>, Error> {
        let socket = self.fetch_client(message.client).await?;
        match socket.send(SocketMessage::OutgoingMessage(message)).await {
            Ok(_) => Ok(None),
            Err(error) => if let SocketMessage::OutgoingMessage(message) = error.0 {
                Ok(Some(message))
            } else {
                Err(Error::from(error))
            },
        }
    }

    pub async fn get_queue_status(&self, queue_name: String) -> Result<QueueStatusResponse, Error> {
        let (send, recv) = oneshot::channel();
        self.connection.send(SessionMessage::GetQueueStatus(queue_name, send)).await?;
        return Ok(recv.await?);
    }
}

type Socket = WebSocketStream<hyper::upgrade::Upgraded>;
enum SocketMessage {
    ResetConnection(Socket),
    OutgoingMessage(ClientMessage),
    PreparedMessage(ClientResponse),
    Stop
}

pub struct Session {
    config: Arc<Configuration>,
    api_handle: JoinHandle<()>,
    command_sink: mpsc::Sender<SessionMessage>,
    command_source: mpsc::Receiver<SessionMessage>,
    connections: HashMap<ClientId, (mpsc::Sender<SocketMessage>, JoinHandle<()>)>,
    queues: HashMap<String, mpsc::Sender<QueueCommand>>,
}

impl Session {
    pub async fn open(config: Configuration) -> Result<Self, Error> {
        let (command_sink, command_source) = mpsc::channel(128);
        // let (outgoing_sink, outgoing_source) = mpsc::channel(128);
        let config = Arc::new(config);

        let client = SessionClient {
            config: config.clone(),
            connection: command_sink.clone(),
            queue_cache: Default::default(),
            client_cache: Default::default(),
        };

        let api_handle = tokio::spawn(run_api(client));

        let mut session = Self {
            config,
            command_sink,
            command_source,
            api_handle,
            connections: Default::default(),
            queues: Default::default(),
        };

        session.load_queues().await?;

        return Ok(session)
    }

    async fn load_queues(&mut self) -> Result<(), Error> {
        let mut dir_listing = tokio::fs::read_dir(&self.config.data_path).await?;
        while let Some(entry) = dir_listing.next_entry().await? {
            if !entry.file_type().await?.is_dir() { continue }
            if let Err(_) = entry.file_name().into_string() {
                continue
            };

            let path = entry.path();
            if !file_exists(&path.join("header")).await.unwrap_or(false) {
                continue
            }

            let (name, queue) = match open_queue(path, self.client()).await {
                Ok(queue) => queue,
                Err(err) => { error!("Error opening queue: {err:?}"); continue },
            };
            self.queues.insert(name, queue);
        }

        for queue in self.config.queues.iter() {
            let (name, queue) = match create_queue(&self.config.data_path, self.client(), queue.clone()).await {
                Ok(data) => data,
                Err(err) => {
                    error!("Error initializing queue: {err}");
                    continue;
                },
            };
            self.queues.insert(name, queue);
        }

        return Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                message = self.command_source.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => return Ok(()),
                    };

                    match self.process_message(message).await {
                        Ok(do_break) => {
                            if do_break {
                                break;
                            }
                        }
                        Err(err) => {
                            error!("Session Error: {err:?}");
                        }
                    }
                }
            }
        }
        return Ok(())
    }

    async fn process_message(&mut self, message: SessionMessage) -> Result<bool, Error> {
        match message {
            SessionMessage::Connection(socket, client_id, durable_session) => self.handle_connection(socket, client_id, durable_session).await?,
            SessionMessage::Stop => {
                // Stop gui
                self.api_handle.abort();

                // Close connections
                for (_, (socket, _)) in self.connections.iter() {
                    let _ = socket.send(SocketMessage::Stop).await;
                }
                for (_, (_, handle)) in self.connections.iter_mut() {
                    if let Err(err) = handle.await {
                        error!("Stopping client thread: {err}");
                    }
                }

                // shutdown queues
                for (_, queue) in self.queues.iter() {
                    let (send, recv) = oneshot::channel();
                    let _ = queue.send(QueueCommand::Stop(send)).await;
                    recv.await?;
                }
                return Ok(true)
            },
            SessionMessage::GetQueue(name, response) => {
                if let Some(sock) = self.queues.get(&name) {
                    let _ = response.send(sock.clone());
                }
            },
            SessionMessage::CreateQueue(client, create) => {
                // Check if we are allowed to create queues
                if !self.config.runtime_create_queues {
                    if let Some((con, _)) = self.connections.get(&client) {
                        let _ = con.send(SocketMessage::PreparedMessage(ClientResponse::Error(ClientError{
                            label: create.label,
                            code: ErrorCode::PermissionDenied,
                            key: create.queue,
                        }))).await;
                    }
                    return Ok(false)
                }

                // Check if the name is already used
                if self.queues.contains_key(&create.queue) {
                    if let Some((con, _)) = self.connections.get(&client) {
                        let _ = con.send(SocketMessage::PreparedMessage(ClientResponse::Error(ClientError{
                            label: create.label,
                            code: ErrorCode::ObjectAlreadyExists,
                            key: create.queue,
                        }))).await;
                    }
                } else {
                    // Create the queue
                    let session_client = SessionClient{
                        config: self.config.clone(),
                        connection: self.command_sink.clone(),
                        queue_cache: Default::default(),
                        client_cache: Default::default(),
                    };

                    let (_, client_con) = create_queue(&self.config.data_path, session_client, QueueConfiguration{
                        name: create.queue.clone(),
                        max_retries: create.retries.unwrap_or(u32::MAX),
                        shard_max_records: create.shard_max_entries.unwrap_or(1 << 16),
                        shard_max_bytes: create.shard_max_bytes.unwrap_or(1 << 30),
                    }).await?;
                    self.queues.insert(create.queue, client_con);

                    // Notify the requester
                    if let Some((con, _)) = self.connections.get(&client) {
                        let _ = con.send(SocketMessage::PreparedMessage(ClientResponse::Notice(ClientNotice {
                            label: create.label,
                            notice: NotificationName::Ready,
                        }))).await;
                    }
                }
            }
            SessionMessage::GetConnection(id, response) => {
                match self.connections.entry(id) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let (sender, _) = entry.get();
                        let _ = response.send(sender.clone());
                    },
                    std::collections::hash_map::Entry::Vacant(_) => (),
                }
            },
            SessionMessage::GetQueueStatus(name, response) => match self.queues.get(&name) {
                Some(queue) => { 
                    if let Err(error) = queue.send(QueueCommand::Status(response)).await {
                        if let QueueCommand::Status(response) = error.0 {
                            let _ = response.send(QueueStatusResponse::Error(QueueStatusError::new(name, QueueStatusErrorKind::QueueNotFound)));
                        }
                    }
                },
                None => { 
                    let _ = response.send(QueueStatusResponse::Error(QueueStatusError::new(name, QueueStatusErrorKind::QueueNotFound))); 
                },
            },
        }
        return Ok(false)
    }

    fn generate_client_id(&self) -> ClientId {
        let mut id = chrono::Utc::now().timestamp_nanos() as u64;
        while self.connections.contains_key(&ClientId(id)) {
            id += 1;
        }
        return ClientId(id)
    }

    pub async fn handle_connection(&mut self, socket: HyperWebsocket, client_id: Option<ClientId>, durable_session: bool) -> Result<(), Error> {
        let client_id = match client_id {
            Some(client) => {
                info!("New connection reusing id: {client}");
                client
            },
            None => {
                let client = self.generate_client_id();
                info!("New connection issued id: {client}");
                client
            },
        };

        let mut socket = match socket.await {
            Ok(socket) => socket,
            Err(err) => {
                error!("{err}");
                return Ok(())
            },
        };

        socket.send(Message::Text(serde_json::to_string(&ClientResponse::Hello(ClientHello{id: client_id.0}))?)).await?;

        if let Some((sink, _)) = self.connections.get_mut(&client_id) {
            socket = match sink.send(SocketMessage::ResetConnection(socket)).await {
                Ok(_) => return Ok(()),
                Err(err) => if let SocketMessage::ResetConnection(sock) = err.0 {
                    sock
                } else {
                    return Ok(())
                },
            }
        }

        let (send, recv) = mpsc::channel(32);
        let worker = tokio::spawn(run_socket(client_id, durable_session, socket, recv, self.client()));
        self.connections.insert(client_id, (send, worker));

        return Ok(())
    }

    pub fn client(&self) -> SessionClient {
        SessionClient {
            connection: self.command_sink.clone(),
            config: self.config.clone(),
            queue_cache: Default::default(),
            client_cache: Default::default()
        }
    }
}


async fn run_socket(client_id: ClientId, durable: bool, mut socket: Socket, mut recv: mpsc::Receiver<SocketMessage>, mut client: SessionClient) {
    info!("Launching client minder: {client_id}");
    let mut buffer: VecDeque<Message> = VecDeque::new();
    let mut socket_spoiled = false;
    loop {
        if socket_spoiled {
            if !durable {
                break;
            }

            let message = match recv.recv().await {
                Some(message) => message,
                None => break,
            };

            let message = match message {
                SocketMessage::Stop => break,
                SocketMessage::ResetConnection(new) => {
                    socket = new;
                    socket_spoiled = false;

                    while let Some(message) = buffer.pop_front() {
                        if let Err(_) = socket.send(message.clone()).await {
                            socket_spoiled = true;
                            buffer.push_front(message);
                            break;
                        }
                    };
                    continue
                },
                SocketMessage::OutgoingMessage(message) => {
                    match ClientResponse::try_from(message){
                        Ok(message) => message,
                        Err(err) => { error!("Send Encode Error: {err}"); continue; },
                    }
                },
                SocketMessage::PreparedMessage(message) => {
                    message
                }
            };

            let message = Message::Text(match serde_json::to_string(&message) {
                Ok(message) => message,
                Err(err) => { error!("Send Error: {err}"); continue },
            });
            buffer.push_back(message);

        } else {
            tokio::select!{
                message = recv.recv() => {
                    let message = match message {
                        Some(message) => message,
                        None => break,
                    };

                    let message = match message {
                        SocketMessage::Stop => break,
                        SocketMessage::ResetConnection(new) => { socket = new; continue },
                        SocketMessage::OutgoingMessage(message) => {
                            match ClientResponse::try_from(message){
                                Ok(message) => message,
                                Err(err) => { error!("Send Encode Error: {err}"); continue },
                            }
                        },
                        SocketMessage::PreparedMessage(message) => {
                            message
                        }
                    };

                    let message = Message::Text(match serde_json::to_string(&message) {
                        Ok(message) => message,
                        Err(err) => { error!("Send Error: {err}"); continue },
                    });

                    if let Err(_) = socket.send(message.clone()).await {
                        socket_spoiled = true;
                        buffer.push_back(message);
                    }
                },

                message = socket.next() => {
                    let message = match message {
                        Some(message) => message,
                        None => {
                            info!("Lost socket for {client_id}");
                            socket_spoiled = true;
                            continue;
                        },
                    };

                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            error!("Socket error: {err:?}");
                            continue;
                        },
                    };

                    let message: Result<ClientRequest, serde_json::Error> = match &message {
                        Message::Text(text) => serde_json::from_str(text),
                        Message::Binary(data) => serde_json::from_slice(data),
                        _ => continue
                    };

                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            error!("json error {err:?}");
                            continue;
                        },
                    };

                    debug!("Handling client message for {client_id} {message:?}");
                    let label = message.label();
                    let key = message.queue();
                    let result = match message {
                        ClientRequest::Post(post) => {
                            // let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(client.config.compression));
                            // if let Err(_) = encoder.write_all(&post.message) {
                            //     continue
                            // }
                            // post.message = match encoder.finish() {
                            //     Ok(message) => message,
                            //     Err(_) => continue,
                            // };
                    
                            client.post(client_id, post).await
                        },
                        ClientRequest::Fetch(fetch) => client.fetch(client_id, fetch).await,
                        ClientRequest::Finish(finish) => client.finish(client_id, finish).await,
                        ClientRequest::Pop(pop) => client.pop(client_id, pop).await,
                        ClientRequest::Create(create) => client.create(client_id, create).await,
                    };

                    if let Err(err) = result {
                        error!("{err:?}");

                        let message = ClientResponse::Error(ClientError{ 
                            label, 
                            code: ErrorCode::NoObject, 
                            key
                        });

                        let message = Message::Text(match serde_json::to_string(&message) {
                            Ok(message) => message,
                            Err(err) => { error!("Send Error: {err}"); continue },
                        });

                        if let Err(_) = socket.send(message.clone()).await {
                            socket_spoiled = true;
                            buffer.push_back(message);
                        }

                    }
                }
            }
        }
    }
    info!("Finishing client minder: {client_id}");
}
