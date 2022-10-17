use std::path::{Path, PathBuf};

use crate::{Api, IpfsPath, OutType};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use relative_path::RelativePathBuf;

#[async_trait(?Send)]
pub trait ApiExt: Api {
    /// High level get, equivalent of CLI `iroh get`
    async fn get<'a>(
        &self,
        ipfs_path: &IpfsPath,
        output_path: Option<&'a Path>,
    ) -> Result<PathBuf> {
        if ipfs_path.cid().is_none() {
            return Err(anyhow!("IPFS path does not refer to a CID"));
        }
        let root_path = get_root_path(ipfs_path, output_path);
        if root_path.exists() {
            return Err(anyhow!(
                "output path {} already exists",
                root_path.display()
            ));
        }
        let blocks = self.get_stream(ipfs_path);
        save_get_stream(&root_path, blocks).await?;
        Ok(root_path)
    }

    async fn add(&self, path: &Path, wrap: bool, recursive: bool) -> Result<Cid> {
        if path.is_dir() {
            if !recursive {
                // TODO(faassen) This error message is okay for the API but not really
                // ideal for the CLI, so need to make it different in the CLI
                anyhow::bail!("{} is a directory, use recursive to add it", path.display());
            }
            self.add_dir(path, wrap).await
        } else if path.is_file() {
            self.add_file(path, wrap).await
        } else {
            anyhow::bail!("can only add files or directories")
        }
    }
}

impl<T> ApiExt for T where T: Api {}

/// take a stream of blocks as from `get_stream` and write them to the filesystem
async fn save_get_stream(
    root_path: &Path,
    blocks: impl Stream<Item = Result<(RelativePathBuf, OutType)>>,
) -> Result<()> {
    tokio::pin!(blocks);
    while let Some(block) = blocks.next().await {
        let (path, out) = block?;
        let full_path = path.to_path(root_path);
        match out {
            OutType::Dir => {
                tokio::fs::create_dir_all(full_path).await?;
            }
            OutType::Reader(mut reader) => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent.to_path(root_path)).await?;
                }
                let mut f = tokio::fs::File::create(full_path).await?;
                tokio::io::copy(&mut reader, &mut f).await?;
            }
        }
    }
    Ok(())
}

/// Given an cid and an optional output path, determine root path
fn get_root_path(ipfs_path: &IpfsPath, output_path: Option<&Path>) -> PathBuf {
    match output_path {
        Some(path) => path.to_path_buf(),
        None => {
            if ipfs_path.tail().is_empty() {
                PathBuf::from(ipfs_path.cid().unwrap().to_string())
            } else {
                PathBuf::from(ipfs_path.tail().last().unwrap())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_save_get_stream() {
        let stream = Box::pin(futures::stream::iter(vec![
            Ok((RelativePathBuf::from_path("a").unwrap(), OutType::Dir)),
            Ok((
                RelativePathBuf::from_path("b").unwrap(),
                OutType::Reader(Box::new(std::io::Cursor::new("hello"))),
            )),
        ]));
        let tmp_dir = TempDir::new("test_save_get_stream").unwrap();
        save_get_stream(tmp_dir.path(), stream).await.unwrap();
        assert!(tmp_dir.path().join("a").is_dir());
        assert_eq!(
            std::fs::read_to_string(tmp_dir.path().join("b")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_get_root_path() {
        let ipfs_path =
            IpfsPath::from_str("/ipfs/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N").unwrap();
        assert_eq!(
            get_root_path(&ipfs_path, None),
            PathBuf::from("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
        );
        assert_eq!(
            get_root_path(&ipfs_path, Some(Path::new("bar"))),
            PathBuf::from("bar")
        );
    }

    #[test]
    fn test_get_root_path_with_tail() {
        let ipfs_path =
            IpfsPath::from_str("/ipfs/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N/tail")
                .unwrap();
        assert_eq!(get_root_path(&ipfs_path, None), PathBuf::from("tail"));
        assert_eq!(
            get_root_path(&ipfs_path, Some(Path::new("bar"))),
            PathBuf::from("bar")
        );
    }
}
