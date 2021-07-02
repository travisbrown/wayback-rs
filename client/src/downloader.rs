use super::Item;
use bytes::Bytes;
use reqwest::{redirect, Client};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("HTTP client error")]
    ClientError(#[from] reqwest::Error),
}

pub struct Downloader {
    client: Client,
    redirect_client: Client,
}

impl Downloader {
    const TCP_KEEPALIVE_SECS: u64 = 20;

    pub fn new() -> reqwest::Result<Self> {
        Ok(Self {
            client: Client::builder()
                .tcp_keepalive(Some(Duration::from_secs(Self::TCP_KEEPALIVE_SECS)))
                .redirect(redirect::Policy::none())
                .build()?,
            redirect_client: Client::builder()
                .tcp_keepalive(Some(Duration::from_secs(Self::TCP_KEEPALIVE_SECS)))
                .build()?,
        })
    }

    pub async fn download(
        &self,
        item: &Item,
        original: bool,
        follow_redirects: bool,
    ) -> reqwest::Result<Bytes> {
        let client = if follow_redirects {
            &self.redirect_client
        } else {
            &self.client
        };
        Ok(client
            .get(&item.wayback_url(original))
            .send()
            .await?
            .bytes()
            .await?)
    }
}
