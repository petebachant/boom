// Database related functionality
use crate::conf::{AppConfig, DatabaseConfig};

use mongodb::{Client, Database};

async fn db_from_config(config: DatabaseConfig) -> Database {
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!(
            "mongodb://{}:{}@{}:{}",
            config.username, config.password, config.host, config.port
        )
        .into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    client.database(&config.name)
}

pub async fn get_db() -> Database {
    // Read the config file
    let config = AppConfig::from_default_path().database;
    db_from_config(config).await
}

pub async fn get_default_db() -> Database {
    let config = AppConfig::default().database;
    db_from_config(config).await
}
