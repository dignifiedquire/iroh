//! Define the tags subcommand.

use anyhow::Result;
use bytes::Bytes;
use clap::Subcommand;
use futures_lite::StreamExt;
use iroh::blobs::Tag;
use iroh::client::Iroh;

/// Commands to manage tags.
#[derive(Subcommand, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum TagCommands {
    /// List all tags
    List,
    /// Delete a tag
    Delete {
        tag: String,
        #[clap(long, default_value_t = false)]
        hex: bool,
    },
}

impl TagCommands {
    /// Runs the tag command given the iroh client.
    pub async fn run(self, iroh: &Iroh) -> Result<()> {
        match self {
            Self::List => {
                let mut response = iroh.tags().list().await?;
                while let Some(res) = response.next().await {
                    let res = res?;
                    println!("{}: {} ({:?})", res.name, res.hash, res.format);
                }
            }
            Self::Delete { tag, hex } => {
                let tag = if hex {
                    Tag::from(Bytes::from(hex::decode(tag)?))
                } else {
                    Tag::from(tag)
                };
                iroh.tags().delete(tag).await?;
            }
        }
        Ok(())
    }
}
