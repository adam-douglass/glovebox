mod error;
mod data_journal;
mod operation_journal;
mod server;
mod session;
mod priority;
mod config;
mod request;
mod response;
#[cfg(test)]
mod test;

use config::Configuration;
use session::Session;


#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() -> anyhow::Result<()>{
    // Parse configuration
    let config: Configuration = confy::load("glovebox")?;
    tokio::fs::create_dir_all(&config.data_path).await?;

    // Start loading server info
    let mut session = Session::open(config).await?;

    // Shutdown the server on ctrl_c
    let client = session.client();
    tokio::spawn(async move {
        if let Ok(_) = tokio::signal::ctrl_c().await {
            let _ = client.stop().await;
        }
    });

    // Run forever
    session.run().await?;
    return Ok(())
}
