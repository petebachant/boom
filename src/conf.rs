use config::Config;
use config::File;
use config::ConfigError;

use crate::types;


pub fn load_config(filepath: &str) -> Result<Config, ConfigError> {
    let conf = Config::builder()
        .add_source(File::with_name(filepath))
        .build()
        .unwrap();
    Ok(conf)
}

pub fn build_xmatch_configs(conf: Config) -> Vec<types::CatalogXmatchConfig> {
    let crossmatches = conf.get_array("crossmatch").unwrap();
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches {
        let catalog_xmatch_config = types::CatalogXmatchConfig::from_config(crossmatch);
        catalog_xmatch_configs.push(catalog_xmatch_config);
    }

    catalog_xmatch_configs
}