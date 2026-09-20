use serde::{Serialize,Deserialize};
use std::collections::BTreeSet;
use std::fs;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config
{
    pub sites : BTreeSet<String>, // set of sites (base URLs thereof) to index
}

const CONFIG_FILE_NAME: &str = "config.toml";

impl Config
{
    pub fn load()->Option<Config> // the caller is assumed to acquire the RwLock guard beforehand
    {
        let config_file_exists = fs::exists(CONFIG_FILE_NAME).unwrap_or(false);
        if !config_file_exists
        {
            println!("load_config: skipped, file '{CONFIG_FILE_NAME}' does not exists.");
            return None;
        }
        let config_content = match fs::read_to_string(CONFIG_FILE_NAME)
        {
            Ok(content) => content,
            Err(err) => { println!("Failed reading '{CONFIG_FILE_NAME}': {err}"); return None}
        };
        let file_config: Config = match toml::from_str(&config_content)
        {
            Ok(file_config) => file_config,
            Err(err) => { println!("Failed deserializing config from '{CONFIG_FILE_NAME}': {err}"); return None}
        };
        println!("Loaded config from '{CONFIG_FILE_NAME}':\n{:?}", file_config);
        return Some(file_config)
    }

    pub fn save(&self) // the caller is assumed to acquire the RwLock guard beforehand
    {
        let config_content_string = match toml::to_string(self)
        {
            Ok(content) => content,
            Err(err) => { println!("Error ({}) while serializing config: {:?}", err, self); return }
        };
        match fs::write(CONFIG_FILE_NAME, config_content_string)
        {
            Ok(_) => { println!("Config written to file {CONFIG_FILE_NAME}") },
            Err(err) => { println!("Error ({err}) while writing config to file {CONFIG_FILE_NAME}"); return }
        };
    }
}
