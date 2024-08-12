use config::Config;
use config::ConfigError;
use config::File;

use crate::types;

pub fn load_config(filepath: &str) -> Result<Config, ConfigError> {
    let conf = Config::builder()
        .add_source(File::with_name(filepath))
        .build()
        .unwrap();
    Ok(conf)
}

pub fn build_xmatch_configs(conf: &Config) -> Vec<types::CatalogXmatchConfig> {
    let crossmatches = conf.get_array("crossmatch").unwrap();
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches {
        let catalog_xmatch_config = types::CatalogXmatchConfig::from_config(crossmatch);
        catalog_xmatch_configs.push(catalog_xmatch_config);
    }

    catalog_xmatch_configs
}

pub async fn build_db(conf: &Config) -> mongodb::Database {
    let db_conf = conf.get_table("db").unwrap();

    let host = {
        if let Some(host) = db_conf.get("host") {
            host.clone().into_string().unwrap()
        } else {
            "localhost".to_string()
        }
    };

    let port = {
        if let Some(port) = db_conf.get("port") {
            port.clone().into_int().unwrap() as u16
        } else {
            27017
        }
    };

    let name = {
        if let Some(name) = db_conf.get("name") {
            name.clone().into_string().unwrap()
        } else {
            "zvar".to_string()
        }
    };

    let uri = format!("mongodb://{}:{}", host, port);
    let client_mongo = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client_mongo.database(&name);
    db
}