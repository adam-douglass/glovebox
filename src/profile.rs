
mod error;
mod data_journal;
mod operation_journal;
mod server;
mod session;
mod priority;
mod config;
mod test;
mod request;

use futures::SinkExt;
use hyper_tungstenite::tungstenite::Message;
use log::{error, info};
use error::Error;
use request::{ClientCreate, ClientRequest, ClientRequestJSON};
use tempdir::TempDir;
use test::{SendMessages, PopMessages};
use tokio_tungstenite::connect_async;

use crate::config::Configuration;
use crate::session::Session;



#[tokio::main]
async fn main() -> Result<(), Error>{
    env_logger::init();
    // Launch the server
    
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
        let outgoing_message = ClientRequestJSON::Create(ClientCreate{
            queue: String::from("test_queue"), 
            retries: Some(retries),
            label: 0,
            shard_max_entries: None,
            shard_max_bytes: None,
        });
        ws.send(Message::Text(serde_json::to_string(&outgoing_message).unwrap())).await.unwrap();
        ws.close(None).await?;

        client
    };
    
    let producer = SendMessages::new(port).total(10000).batch(2).start();
    let consumer = PopMessages::new(port).total(10000).concurrent(50).start();

    match producer.await.unwrap() {
        Err(err) => error!("Error: {err}"),
        Ok(time) => info!("Produce: {time}s"),
    };
    match consumer.await.unwrap() {
        Err(err) => error!("Error: {err}"),
        Ok(time) => info!("Consumer: {time}s")
    };

    client.stop().await.unwrap();
    return Ok(())
}

