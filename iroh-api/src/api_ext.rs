use std::path::{Path, PathBuf};

use crate::{Api, Cid, IpfsPath, OutType};
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
        let cid = ipfs_path
            .cid()
            .ok_or_else(|| anyhow!("IPFS path does not refer to a CID"))?;
        let root_path = get_root_path(cid, output_path);
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
fn get_root_path(cid: &Cid, output_path: Option<&Path>) -> PathBuf {
    match output_path {
        Some(path) => path.to_path_buf(),
        None => PathBuf::from(cid.to_string()),
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
        let cid = Cid::from_str("QmYbcW4tXLXHWw753boCK8Y7uxLu5abXjyYizhLznq9PUR").unwrap();
        assert_eq!(
            get_root_path(&cid, None),
            PathBuf::from("QmYbcW4tXLXHWw753boCK8Y7uxLu5abXjyYizhLznq9PUR")
        );
        assert_eq!(
            get_root_path(&cid, Some(Path::new("bar"))),
            PathBuf::from("bar")
        );
    }
}
