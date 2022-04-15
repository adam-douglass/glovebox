use futures::{SinkExt, StreamExt};
use hyper_tungstenite::tungstenite::Message;
use log::{info, debug};
use tokio_tungstenite::connect_async;

use crate::error::Error;
use crate::priority::entry::Firmness;
use crate::session::ClientResponse;
use crate::request::{NotificationName, ClientPost, ClientPop, ClientRequest, ClientRequestJSON};


pub struct SendMessages {
    port: u16,
    total: u64,
    batch: u64,
}

impl SendMessages {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            total: 100,
            batch: 1
        }
    }

    pub fn total(mut self, total: u64) -> Self { self.total = total; self }
    pub fn batch(mut self, batch: u64) -> Self { self.batch = batch; self }
    // fn drop(mut self, drop: f32) -> Self { self.drop = drop; self }

    pub fn start(self) -> tokio::task::JoinHandle<Result<f32, Error>> {
        tokio::spawn(send_messages(self.port, self.total, self.batch))
    }
}

async fn send_messages(port: u16, total: u64, batch_size: u64) -> Result<f32, Error> {
    let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await?;
    if let Message::Text(body) = ws.next().await.unwrap()? {
        let hello: ClientResponse = serde_json::from_str(&body)?;
        if let ClientResponse::Hello(client) = hello {
            info!("Producer connected as {}", client.id);
        }
    }

    let rounds = total/batch_size;
    let mut label = 0;
    let starting_time = std::time::Instant::now();

    for round in 0..rounds {
        
        let mut sent = vec![];
        for item in 0..batch_size {
            debug!("+{round}/{rounds} {item}/{batch_size}");

            let outgoing_message = ClientRequest::Post(ClientPost{
                queue: "test_queue".to_owned(),
                message: "TEST MESSAGE".as_bytes().to_vec().into(),
                priority: 1,
                label,
                notify: vec![NotificationName::Ready, NotificationName::Finish]
            });
            sent.push(label);
            ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();
            label += 1;
        }

        while sent.len() > 0 {
            let confirm = ws.next().await.unwrap()?;
            if let Message::Text(body) = confirm {
                let confirm: ClientResponse = serde_json::from_str(&body)?;
                if let ClientResponse::Notice(notice) = confirm {
                    if let Some(index) = sent.iter().position(|&i| i == notice.label) {
                        sent.swap_remove(index);
                    }
                }
            }
        }
    }

    let finish = starting_time.elapsed().as_secs_f32();

    ws.close(None).await?;
    return Ok(finish);
}

// struct FetchMessages {
//     port: u16,
//     drop: f32,
//     limit: i32,
//     concurrent: u32
// }

// impl FetchMessages {
//     fn new(port: u16) -> Self {
//         Self {
//             port,
//             drop: 0.0,
//             limit: 100,
//             concurrent: 1,
//         }
//     }

//     fn limit(mut self, limit: i32) -> Self { self.limit = limit; self }
//     fn concurrent(mut self, concurrent: u32) -> Self { self.concurrent = concurrent; self }
//     fn drop(mut self, drop: f32) -> Self { self.drop = drop; self }

//     fn start(self) -> tokio::task::JoinHandle<Result<FetchResult, Error>> {
//         tokio::spawn(fetch_messages(self.port, self.drop, self.limit, self.concurrent))
//     }
// }

// struct FetchResult {
//     fetches: i32,
//     drops: i32,
//     fetched: i32,
//     finish: i32,
// }

// async fn fetch_messages(port: u16, drop: f32, limit: i32, concurrent: u32) -> Result<FetchResult, Error> {
//     let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await?;
//     if let Message::Text(body) = ws.next().await.unwrap()? {
//         let hello: ClientResponse = serde_json::from_str(&body)?;
//         if let ClientResponse::Hello(client) = hello {
//             info!("Consumer connected as {}", client.id);
//         }
//     }
//     let mut ii = 0;
//     let mut outstanding = HashSet::new();
//     let mut drop_count = 0;
//     let mut fetch_count = 0;
//     let mut fetched_count = 0;
//     let mut finish_count = 0;
//     let queue = String::from("test_queue");

//     while finish_count < limit {
//         while outstanding.len() < concurrent as usize {
//             ii += 1;
//             outstanding.insert(ii);
//             let outgoing_message = ClientRequest::Fetch(ClientFetch{queue: queue.clone(), label:ii,sync:Firmness::Write, blocking: true, work_timeout: 0.5, block_timeout: 30.0 });
//             fetch_count += 1;
//             ws.send(Message::Text(serde_json::to_string(&outgoing_message)?)).await.unwrap();
//         }
//         let confirm = ws.next().await.unwrap()?;
//         if let Message::Text(body) = confirm {
//             let confirm: ClientResponse = serde_json::from_str(&body)?;
//             match confirm  {
//                 ClientResponse::Message(message) => {
//                     assert!(outstanding.remove(&message.label));
//                     assert_eq!(message.body, b"TEST MESSAGE");
//                     fetched_count += 1;

//                     if thread_rng().gen::<f32>() > drop {
//                         let outgoing_message = ClientRequest::Finish(ClientFinish{
//                             queue: queue.clone(), 
//                             shard: message.shard, 
//                             sequence: message.sequence,
//                             label: None,
//                             response: None,
//                         });
//                         ws.send(Message::Text(serde_json::to_string(&outgoing_message)?)).await.unwrap();
//                         finish_count +=1;
//                         info!("finished {finish_count}");
//                     } else {
//                         drop_count += 1;
//                     }
//                 }
//                 _ => {

//                     continue
//                 }
//             }
//         }
//     }
//     ws.close(None).await?;

//     return Ok(FetchResult{
//         fetched: fetched_count,
//         fetches: fetch_count,
//         drops: drop_count,
//         finish: finish_count,
//     });
// }


pub struct PopMessages {
    port: u16,
    total: u64,
    concurrent: u64,
}

impl PopMessages {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            total: 100,
            concurrent: 1
        }
    }

    pub fn total(mut self, total: u64) -> Self { self.total = total; self }
    pub fn concurrent(mut self, concurrent: u64) -> Self { self.concurrent = concurrent; self }
    // fn drop(mut self, drop: f32) -> Self { self.drop = drop; self }

    pub fn start(self) -> tokio::task::JoinHandle<Result<f32, Error>> {
        tokio::spawn(pop_messages(self.port, self.total, self.concurrent))
    }
}

async fn pop_messages(port: u16, total: u64, concurrent: u64) -> Result<f32, Error> {
    let (mut ws, _) = connect_async(format!("ws://localhost:{port}/connect/")).await?;
    if let Message::Text(body) = ws.next().await.unwrap()? {
        let hello: ClientResponse = serde_json::from_str(&body)?;
        if let ClientResponse::Hello(client) = hello {
            info!("Producer connected as {}", client.id);
        }
    }

    let mut popped = 0;
    let mut outstanding = 0;
    let mut label = 0;
    let queue = String::from("test_queue");
    let starting_time = std::time::Instant::now();

    while popped < total {
        debug!("-{}/{}", popped, total);

        let to_add = (concurrent - outstanding).min(total - popped);
        for _ in 0..to_add {
            outstanding += 1;            
            let outgoing_message = ClientRequest::Pop(ClientPop{
                queue: queue.clone(), label, sync:Firmness::Write, 
                blocking: true, block_timeout: 30.0 
            });
            label += 1;
            ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message))?)).await.unwrap();
        }

        let confirm = ws.next().await.unwrap()?;
        if let Message::Text(body) = confirm {
            let confirm: ClientResponse = serde_json::from_str(&body)?;
            match confirm {
                ClientResponse::Message(_) => {
                    popped += 1;
                    outstanding -= 1;
                }
                _ => {}
            }
        }
    }

    let finish = starting_time.elapsed().as_secs_f32();

    ws.close(None).await?;
    return Ok(finish);
}


#[cfg(test)]
mod test {
    use futures::SinkExt;
    use hyper_tungstenite::tungstenite::Message;
    use log::error;
    use tempdir::TempDir;
    use tokio_tungstenite::connect_async;
    use crate::session::Session;
    use crate::config::Configuration;
    use crate::request::{ClientCreate, ClientRequest, ClientRequestJSON};

    use super::{SendMessages, PopMessages};

    #[tokio::test]
    async fn profile() {
        
        let port = 5000;
        let retries = 3;
        let temp = TempDir::new("profile").unwrap();
        let client = {
            let mut session = Session::open(Configuration {
                data_path: temp.path().to_path_buf(),
                bind_address: "127.0.0.1".to_string(),
                port,
                compression: 0,
                runtime_create_queues: true,
                queues: vec![],
            }).await.unwrap();
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
                shard_max_entries: None,
                shard_max_bytes: None,
            });
            ws.send(Message::Text(serde_json::to_string(&ClientRequestJSON::from(outgoing_message)).unwrap())).await.unwrap();
            ws.close(None).await.unwrap();

            client
        };
        
        let producer = SendMessages::new(port).total(10000).batch(2).start();
        let consumer = PopMessages::new(port).total(10000).concurrent(50).start();

        producer.await.unwrap().unwrap();
        consumer.await.unwrap().unwrap();

        client.stop().await.unwrap();
    }
}