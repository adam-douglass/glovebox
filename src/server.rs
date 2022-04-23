use std::convert::Infallible;
use std::net::{SocketAddr, Ipv4Addr};
use std::str::FromStr;

use hyper::{Body, Server, Request, Response, StatusCode};
use log::{info, error};
use routerify::ext::RequestExt;
use routerify::{Router, RouterService};

use crate::priority::entry::ClientId;
use crate::session::SessionClient;

async fn respond_error(error: String) -> Result<Response<Body>, Infallible> {
    let mut response = Response::new(Body::from(error));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    Ok(response)
}

async fn queue_status_handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let session = match req.data::<SessionClient>() {
        Some(client ) => client.clone(),
        None => return respond_error(format!("Internal error")).await,
    };
    let queue_name = match req.param("queue_name") {
        Some(name ) => name,
        None => return respond_error(format!("Bad queue name")).await,
    };

    let status = match session.get_queue_status(queue_name.clone()).await {
        Ok(status ) => status,
        Err(_) => return respond_error(format!("Queue status not found")).await,
    };

    let data = match serde_json::to_string(&status) {
        Ok(data) => data,
        Err(err) => return respond_error(format!("Encoding error: {err}")).await,
    };

    Ok(Response::new(Body::from(data)))
}


async fn setup_connection(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let session = match req.data::<SessionClient>() {
        Some(client ) => client.clone(),
        None => return respond_error(format!("Internal error")).await,
    };

    let client_id = match req.headers().get("RECONNECT-AS") {
        Some(header) => match header.to_str() {
            Ok(header) => match header.parse() {
                Ok(client_id) => Some(ClientId(client_id)),
                Err(_) => None,
            },
            Err(_) => None,
        },
        None => None,
    };

    let durable_session = match req.headers().get("DURABLE-SESSION") {
        Some(header) => match header.to_str() {
            Ok(header) => {
                let header = header.to_lowercase();
                header == "1" || header == "true"
            },
            Err(_) => false,
        },
        None => false,
    };

    let binary = match req.headers().get("Binary") {
        Some(header) => match header.to_str() {
            Ok(header) => {
                let header = header.to_lowercase();
                header == "1" || header == "true"
            },
            Err(_) => false,
        },
        None => false,
    };

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&req) {
        let (response, websocket) = match hyper_tungstenite::upgrade(req, None) {
            Ok(resp) => resp,
            Err(err) => {
                return respond_error(format!("{err}")).await
            },
        };

        // Spawn a task to handle the websocket connection.
        if let Err(e) = session.serve_websocket(websocket, client_id, durable_session, binary).await {
            error!("Error in websocket connection: {:?}", e);
        }

        // Return the response so the spawned future can continue.
        Ok(response)
    } else {
        // Handle regular HTTP requests here.
        return respond_error("Websocket request expected".to_owned()).await
    }
}

// Create a `Router<Body, Infallible>` for response body type `hyper::Body`
// and for handler error type `Infallible`.
fn router(session: SessionClient) -> Router<Body, Infallible> {
    // Create a router and specify the logger middleware and the handlers.
    // Here, "Middleware::pre" means we're adding a pre middleware which will be executed
    // before any route handlers.
    Router::builder()
        // Specify the state data which will be available to every route handlers,
        // error handler and middlewares.
        .data(session)
        // .middleware(Middleware::pre(logger))
        .get("/status/:queue_name", queue_status_handler)
        .get("/connect/", setup_connection)
        // .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}


pub async fn run_api(session: SessionClient) {

    let addr = match std::net::Ipv4Addr::from_str(&session.config.bind_address) {
        Ok(addr) => addr,
        Err(err) => {
            let bind = &session.config.bind_address;
            error!("Couldn't parse bind address {bind} {err}. Binding localhost instead.");
            Ipv4Addr::LOCALHOST
        },
    };

    let addr = SocketAddr::from((addr, session.config.port));

    // Create a Service from the router above to handle incoming requests.
    let router = router(session);
    let service = RouterService::new(router).unwrap();

    info!("Binding {addr}");
    let server = Server::bind(&addr).serve(service);

    // Run this server for... forever!
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}


#[cfg(test)]
mod test {

    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use futures_util::SinkExt;
    use hyper::body::HttpBody;
    use hyper_tungstenite::tungstenite::Message;

    use log::{error, info};
    use rand::{thread_rng, Rng};
    use tempdir::TempDir;
    use tokio::sync::mpsc;
    use tokio_tungstenite::connect_async;
    use futures_util::stream::StreamExt;
    use crate::priority::broker::QueueStatus;
    use crate::priority::entry::Firmness;
    use crate::response::{ClientResponseJSON, ErrorCode};
    use crate::session::{Session, SessionClient};
    use crate::request::{ClientRequest, ClientPost, ClientFetch, ClientFinish, NotificationName, ClientCreate, ClientRequestJSON, MessageJSON};
    
    use crate::config::{Configuration, QueueConfiguration};

    struct SendMessages {
        port: u16,
        total: u64,
        concurrent: usize,
    }

    impl SendMessages {
        fn new(port: u16) -> Self {
            Self {
                port,
                total: 100,
                concurrent: 1
            }
        }

        fn total(mut self, total: u64) -> Self { self.total = total; self }
        fn concurrent(mut self, concurrent: usize) -> Self { self.concurrent = concurrent; self }
        // fn drop(mut self, drop: f32) -> Self { self.drop = drop; self }

        fn start(self) -> tokio::task::JoinHandle<anyhow::Result<u64>> {
            tokio::spawn(send_messages(self.port, self.total, self.concurrent))
        }
    }

    async fn send_messages(port: u16, total: u64, concurrent: usize) -> anyhow::Result<u64> {
        let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await?;
        if let Message::Text(body) = ws.next().await.unwrap()? {
            let hello: ClientResponseJSON = serde_json::from_str(&body)?;
            if let ClientResponseJSON::Hello(client) = hello {
                info!("Producer connected as {}", client.id);
            }
        }
        let mut count = 0;
        let (free_lock, mut get_lock) = mpsc::channel::<()>(concurrent + 1);
        for _ in 0..concurrent {
            free_lock.send(()).await?;
        }
        let labels = std::sync::Arc::new(Mutex::new(HashSet::<u64>::new()));
        let send_labels = labels.clone();

        let (mut send, mut recv) = ws.split();
        let sender_handle = tokio::spawn(async move {
            for ii in 1 ..= total {
                let _ = get_lock.recv().await;
                let outgoing_message = ClientRequest::Post(ClientPost{
                    queue: "test_queue".to_owned(),
                    message: b"TEST MESSAGE".to_vec().into(),
                    priority: 1,
                    label: ii,
                    notify: vec![NotificationName::Write]
                });
                send_labels.lock().unwrap().insert(ii);
                send.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();
            };
            send
        });

        for _ii in 1 ..= total {
            let confirm = recv.next().await.unwrap()?;
            if let Message::Text(body) = confirm {
                let confirm: ClientResponseJSON = serde_json::from_str(&body)?;
                if let ClientResponseJSON::Notice(notice) = confirm {
                    if let NotificationName::Write = notice.notice {
                        assert!(labels.lock().unwrap().remove(&notice.label));
                    } else {
                        panic!("unexpected notice {:?}", notice);
                    }
                    
                    count += 1;
                    let _ = free_lock.send(()).await;
                } else {
                    return Ok(count)
                }
            } else {
                return Ok(count)
            }
        }

        let send = sender_handle.await.unwrap();
        let mut ws = send.reunite(recv).unwrap();
        ws.close(None).await?;
        return Ok(count);
    }

    struct FetchMessages {
        port: u16,
        drop: f32,
        limit: i32,
        concurrent: u32
    }

    impl FetchMessages {
        fn new(port: u16) -> Self {
            Self {
                port,
                drop: 0.0,
                limit: 100,
                concurrent: 1,
            }
        }

        fn limit(mut self, limit: i32) -> Self { self.limit = limit; self }
        fn concurrent(mut self, concurrent: u32) -> Self { self.concurrent = concurrent; self }
        fn drop(mut self, drop: f32) -> Self { self.drop = drop; self }

        fn start(self) -> tokio::task::JoinHandle<anyhow::Result<FetchResult>> {
            tokio::spawn(fetch_messages(self.port, self.drop, self.limit, self.concurrent))
        }
    }

    struct FetchResult {
        fetches: i32,
        drops: i32,
        fetched: i32,
        finish: i32,
    }

    async fn fetch_messages(port: u16, drop: f32, limit: i32, concurrent: u32) -> anyhow::Result<FetchResult> {
        let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await?;
        if let Message::Text(body) = ws.next().await.unwrap()? {
            let hello: ClientResponseJSON = serde_json::from_str(&body)?;
            if let ClientResponseJSON::Hello(client) = hello {
                info!("Consumer connected as {}", client.id);
            }
        }
        let mut ii = 0;
        let mut outstanding = HashSet::new();
        let mut drop_count = 0;
        let mut fetch_count = 0;
        let mut fetched_count = 0;
        let mut finish_count = 0;
        let queue = String::from("test_queue");

        while finish_count < limit {
            while outstanding.len() < concurrent as usize {
                ii += 1;
                outstanding.insert(ii);
                let outgoing_message = ClientRequest::Fetch(ClientFetch{queue: queue.clone(), label:ii,sync:Firmness::Write, work_timeout: 0.5, block_timeout: Some(30.0) });
                fetch_count += 1;
                ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message))?)).await.unwrap();
            }
            let confirm = ws.next().await.unwrap()?;
            if let Message::Text(body) = confirm {
                let confirm: ClientResponseJSON = serde_json::from_str(&body)?;
                match confirm  {
                    ClientResponseJSON::Message(message) => {
                        assert!(outstanding.remove(&message.label));
                        assert_eq!(message.body, MessageJSON::String("TEST MESSAGE".to_string()));
                        fetched_count += 1;

                        if thread_rng().gen::<f32>() > drop {
                            let outgoing_message = ClientRequest::Finish(ClientFinish{
                                queue: queue.clone(), 
                                shard: message.shard, 
                                sequence: message.sequence,
                                label: None,
                                response: None,
                            });
                            ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message))?)).await.unwrap();
                            finish_count +=1;
                            info!("finished {finish_count}");
                        } else {
                            drop_count += 1;
                        }
                    }
                    _ => {

                        continue
                    }
                }
            }
        }
        ws.close(None).await?;

        return Ok(FetchResult{
            fetched: fetched_count,
            fetches: fetch_count,
            drops: drop_count,
            finish: finish_count,
        });
    }

    async fn setup(path: PathBuf, port: u16) -> SessionClient {
        return setup_retries(path, port, 1000).await
    }

    async fn setup_retries(path: PathBuf, port: u16, retries: u32) -> SessionClient {
        let mut config: Configuration = Default::default();
        config.data_path = path;
        config.port = port;

        let mut session = Session::open(config).await.unwrap();
        let client = session.client();

        tokio::spawn(async move {
            if let Err(err) = session.run().await {
                error!("{err:?}");
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await.unwrap();
        let outgoing_message = ClientRequest::Create(ClientCreate{
            queue: String::from("test_queue"), 
            retries: Some(retries),
            label: 0,
            shard_max_entries: Some(80),
            shard_max_bytes: None,
        });
        ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();

        return client;
    }

    #[tokio::test]
    async fn simple_exchange() {
        // Launch the server
        let _ = env_logger::builder().is_test(true).try_init();
        let port = 4000;
        let temp = TempDir::new("simple_exchange").unwrap();
        let client = setup(temp.path().to_path_buf(), port,).await;

        let producer = SendMessages::new(port).start();
        let consumer = FetchMessages::new(port).start();

        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), producer).await.unwrap().unwrap().unwrap(), 100);
        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await.unwrap().unwrap().unwrap().fetched, 100);

        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn faulty() {
        // Launch the server
        let _ = env_logger::builder().is_test(true).try_init();
        let port = 4010;
        let temp = TempDir::new("simple_exchange").unwrap();
        let client = setup(temp.path().to_path_buf(), port).await;

        let producer = SendMessages::new(port).start();
        let consumer = FetchMessages::new(port).drop(0.5).start();

        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), producer).await.unwrap().unwrap().unwrap(), 100);
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await.unwrap().unwrap().unwrap();
        assert_eq!(result.finish, 100);
        assert!(result.drops > 0);
        assert!(result.fetches > 100);

        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn shard_rotation() {
        // Launch the server
        let _ = env_logger::builder().is_test(true).try_init();
        let port = 4011;
        let temp = TempDir::new("simple_exchange").unwrap();
        let client = setup(temp.path().to_path_buf(), port).await;

        let producer = SendMessages::new(port).total(250).concurrent(10).start();
        let consumer = FetchMessages::new(port).limit(250).concurrent(10).start();

        let mut timeout = std::time::Duration::from_secs(60);
        for (key, value) in std::env::vars() {
            if key == "CI" && value == "true" {
                timeout = std::time::Duration::from_secs(600);
            }
        }

        assert_eq!(tokio::time::timeout(timeout, producer).await.unwrap().unwrap().unwrap(), 250);
        let result = tokio::time::timeout(timeout, consumer).await.unwrap().unwrap().unwrap();
        assert_eq!(result.finish, 250);
        assert!(result.drops == 0);

        // Still inside `async fn main`...
        let http_client = hyper::Client::new();
        let mut resp = http_client.get(format!("http://localhost:{port}/status/test_queue").parse().unwrap()).await.unwrap();
        let body = resp.data().await.unwrap().unwrap();
        let status: QueueStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.length, 0);
        assert!(status.shards_finished > 0);

        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn cached_exchange() {
        // Launch the server
        let port = 4001;
        let _ = env_logger::builder().is_test(true).try_init();
        let temp = TempDir::new("simple_exchange").unwrap();
        let client = setup(temp.path().to_path_buf(), port).await;

        let producer = SendMessages::new(port).start();
        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), producer).await.unwrap().unwrap().unwrap(), 100);

        let consumer = FetchMessages::new(port).start();
        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await.unwrap().unwrap().unwrap().fetched, 100);

        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stop_start() {
        // Launch the server
        let port = 4002;
        let _ = env_logger::builder().is_test(true).try_init();
        let temp = TempDir::new("simple_exchange").unwrap();

        {
            let client = setup(temp.path().to_path_buf(), port).await;

            let producer = SendMessages::new(port).start();
            assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), producer).await.unwrap().unwrap().unwrap(), 100);
            client.stop().await.unwrap();
        }

        {
            let client = setup(temp.path().to_path_buf(), port).await;

            let consumer = FetchMessages::new(port).start();
            assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await.unwrap().unwrap().unwrap().fetched, 100);
            client.stop().await.unwrap();
        }

    }

    #[tokio::test]
    async fn test_retries() {
        // Launch the server
        let port = 4003;
        let _ = env_logger::builder().is_test(true).try_init();
        let temp = TempDir::new("simple_exchange").unwrap();

        let client = setup_retries(temp.path().to_path_buf(), port, 3).await;
        let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await.unwrap();
        if let Message::Text(body) = ws.next().await.unwrap().unwrap() {
            let hello: ClientResponseJSON = serde_json::from_str(&body).unwrap();
            if let ClientResponseJSON::Hello(client) = hello {
                info!("test connected as {}", client.id);
            }
        }

        // Send a message
        let label = 5;
        let outgoing_message = ClientRequest::Post(ClientPost{
            queue: "test_queue".to_owned(),
            message: b"TEST MESSAGE".to_vec(),
            priority: 1,
            label,
            notify: vec![NotificationName::Retry, NotificationName::Drop]
        });
        ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();

        // Fetch three times
        for _ in 0..3 {
            let outgoing_message = ClientRequest::Fetch(ClientFetch{
                queue: "test_queue".to_owned(), 
                label: 9,
                sync: Firmness::Write, 
                work_timeout: 0.1, 
                block_timeout: Some(5.0) 
            });
            ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();
        }
        
        // Wait for failure message
        let mut retries = 0;
        let mut drops = 0;
        loop {
            let message = match tokio::time::timeout(std::time::Duration::from_secs(30), ws.next()).await {
                Ok(message) => message.unwrap().unwrap(),
                Err(_) => break,
            };
            if let Message::Text(body) = message {
                let confirm: ClientResponseJSON = serde_json::from_str(&body).unwrap();
                match confirm {
                    ClientResponseJSON::Notice(note)=>{
                        assert_eq!(note.label, label);
                        if note.notice == NotificationName::Retry {
                            info!("retry notice");
                            retries += 1;
                        } else if note.notice == NotificationName::Drop {
                            info!("drop notice");
                            drops += 1;
                        } else {
                            assert!(false);
                        }
                    }
                    ClientResponseJSON::Message(_) => {
                        info!("drop message, we want server to retry");
                    },
                    ClientResponseJSON::Hello(_) => todo!("hello"),
                    ClientResponseJSON::NoMessage(_) => todo!(),
                    ClientResponseJSON::Error(_) => todo!(), 
                }
            }
        }
        assert_eq!(retries, 3);
        assert_eq!(drops, 1);
        
        client.stop().await.unwrap()
    }

    #[tokio::test]
    async fn no_create() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp = TempDir::new("simple_exchange").unwrap();
        let port = 4019;
        let mut session = Session::open(Configuration {
            data_path: temp.path().to_path_buf(),
            bind_address: "127.0.0.1".to_string(),
            port,
            compression: 0,
            runtime_create_queues: false,
            queues: vec![QueueConfiguration { 
                name: "test_queue".to_string(), 
                max_retries: 5, 
                shard_max_records: 100, 
                shard_max_bytes: 50000 
            }],
        }).await.unwrap();
        let client = session.client();

        tokio::spawn(async move {
            if let Err(err) = session.run().await {
                error!("{err:?}");
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await.unwrap();
        if let Message::Text(body) = ws.next().await.unwrap().unwrap() {
            let hello: ClientResponseJSON = serde_json::from_str(&body).unwrap();
            if let ClientResponseJSON::Hello(client) = hello {
                info!("test connected as {}", client.id);
            }
        }

        let outgoing_message = ClientRequest::Create(ClientCreate{
            queue: String::from("test_queue_other"), 
            retries: Some(5),
            label: 99,
            shard_max_entries: Some(80),
            shard_max_bytes: None,
        });
        ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();
        if let Some(Ok(Message::Text(message))) = ws.next().await {
            let response: ClientResponseJSON = serde_json::from_str(&message).unwrap();
            match response {
                ClientResponseJSON::Error(error) => {
                    assert_eq!(error.label, 99);
                    assert_eq!(error.key, String::from("test_queue_other"));
                    assert_eq!(error.code, ErrorCode::PermissionDenied);
                }
                _ => { assert!(false) }
            }
        } else {
            assert!(false)
        }
        ws.close(None).await.unwrap();

        let producer = SendMessages::new(port).start();
        let consumer = FetchMessages::new(port).start();

        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), producer).await.unwrap().unwrap().unwrap(), 100);
        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(10), consumer).await.unwrap().unwrap().unwrap().fetched, 100);

        client.stop().await.unwrap();
    }
}