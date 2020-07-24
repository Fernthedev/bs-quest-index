use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub port: u16,
    pub database_url: String,
    pub downloads_path: PathBuf,
    pub log_level: Option<String>,
}

impl Config {
    pub async fn read<P: AsRef<Path>>(path: P) -> anyhow::Result<&'static Self> {
        let contents = fs::read_to_string(path).await?;
        let config = serde_json::from_str(&contents)?;
        Ok(Box::leak(Box::new(config)))
    }
}
