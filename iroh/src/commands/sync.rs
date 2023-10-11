use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use colored::Colorize;
use dialoguer::Confirm;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use tokio::{io::AsyncReadExt, task::JoinHandle};

use iroh::{
    client::quic::{Doc, Iroh},
    rpc_protocol::{
        DocSetStreamUpdate, DocTicket, ProviderRequest, ProviderResponse, ProviderService,
        ShareMode, WrapOption,
    },
    sync_engine::{LiveEvent, Origin},
};
use iroh_bytes::{
    provider::AddProgress,
    util::{SetTagOption, Tag},
    Hash,
};
use iroh_sync::{store::GetFilter, AuthorId, Entry, NamespaceId};

use iroh_sync::DocSetProgress;
use quic_rpc::{client::UpdateSink, transport::quinn::QuinnConnection};

use crate::config::ConsoleEnv;

const MAX_DISPLAY_CONTENT_LEN: u64 = 80;

#[derive(Debug, Clone, Copy, clap::ValueEnum, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum DisplayContentMode {
    /// Displays the content if small enough, otherwise it displays the content hash.
    Auto,
    /// Display the content unconditionally.
    Content,
    /// Display the hash of the content.
    Hash,
}

#[derive(Debug, Clone, Parser)]
pub enum DocCommands {
    /// Set the active document (only works within the Iroh console).
    Switch { id: NamespaceId },
    /// Create a new document.
    New {
        /// Switch to the created document (only in the Iroh console).
        #[clap(long)]
        switch: bool,
    },
    /// Join a document from a ticket.
    Join {
        ticket: DocTicket,
        /// Switch to the joined document (only in the Iroh console).
        #[clap(long)]
        switch: bool,
    },
    /// List documents.
    List,
    /// Share a document with peers.
    Share {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        mode: ShareMode,
    },
    /// Set an entry in a document.
    Set {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also set with `author switch`.
        #[clap(short, long)]
        author: Option<AuthorId>,
        /// Key to the entry (parsed as UTF-8 string).
        key: String,
        /// Content to store for this entry (parsed as UTF-8 string)
        value: String,
    },
    /// Get entries in a document.
    ///
    /// Shows the author, content hash and content length for all entries for this key.
    Get {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Key to the entry (parsed as UTF-8 string).
        key: String,
        /// If true, get all entries that start with KEY.
        #[clap(short, long)]
        prefix: bool,
        /// Filter by author.
        #[clap(short, long)]
        author: Option<AuthorId>,
        /// How to show the contents of the key.
        #[clap(short, long, default_value_t=DisplayContentMode::Auto)]
        mode: DisplayContentMode,
    },
    /// Delete all entries below a key prefix.
    Del {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also set with `author switch`.
        #[clap(short, long)]
        author: Option<AuthorId>,
        /// Prefix to delete. All entries whose key starts with or is equal to the prefix will be
        /// deleted.
        prefix: String,
    },
    /// List all keys in a document.
    #[clap(alias = "ls")]
    Keys {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Filter by author.
        #[clap(short, long)]
        author: Option<AuthorId>,
        /// Optional key prefix (parsed as UTF-8 string)
        prefix: Option<String>,
        /// How to show the contents of the keys.
        #[clap(short, long, default_value_t=DisplayContentMode::Auto)]
        mode: DisplayContentMode,
    },
    /// Import data into a document
    Import {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also be set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Author of the entry.
        ///
        /// Required unless the author is set through the IROH_AUTHOR environment variable.
        /// Within the Iroh console, the active author can also be set with `author switch`.
        #[clap(short, long)]
        author: Option<AuthorId>,
        /// Prefix to add to imported entries (parsed as UTF-8 string). Defaults to no prefix
        #[clap(long)]
        prefix: Option<String>,
        /// Path to a local file or directory to import
        ///
        /// Pathnames will be used as the document key
        #[clap(long)]
        path: String,
        /// If true, don't copy the file into iroh, reference the existing file instead
        ///
        /// Moving a file imported with `in-place` will result in data corruption
        #[clap(short, long)]
        in_place: bool,
    },
    /// Export data from a document
    Export {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also be set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
        /// Key to the entry (parsed as UTF-8 string)
        #[clap(short, long)]
        key: String,
        /// Path to export to
        #[clap(short, long)]
        path: String,
    },
    /// Watch for changes and events on a document
    Watch {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        #[clap(short, long)]
        doc: Option<NamespaceId>,
    },
    /// Stop syncing a document.
    Leave {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        doc: Option<NamespaceId>,
    },
    /// Delete a document from the local node.
    ///
    /// This is a destructive operation. Both the document secret key and all entries in the
    /// document will be permanently deleted from the node's storage. Content blobs will be deleted
    /// through garbage collection unless they are referenced from another document or tag.
    Drop {
        /// Document to operate on.
        ///
        /// Required unless the document is set through the IROH_DOC environment variable.
        /// Within the Iroh console, the active document can also set with `doc switch`.
        doc: Option<NamespaceId>,
    },
}

#[derive(Debug, Clone, Parser)]
pub enum AuthorCommands {
    /// Set the active author (only works within the Iroh console).
    Switch { author: AuthorId },
    /// Create a new author.
    New {
        /// Switch to the created author (only in the Iroh console).
        #[clap(long)]
        switch: bool,
    },
    /// List authors.
    #[clap(alias = "ls")]
    List,
}

impl DocCommands {
    pub async fn run(self, iroh: &Iroh, env: &ConsoleEnv) -> Result<()> {
        match self {
            Self::Switch { id: doc } => {
                env.set_doc(doc)?;
                println!("Active doc is now {}", fmt_short(doc.as_bytes()));
            }
            Self::New { switch } => {
                if switch && !env.is_console() {
                    bail!("The --switch flag is only supported within the Iroh console.");
                }

                let doc = iroh.docs.create().await?;
                println!("{}", doc.id());

                if switch {
                    env.set_doc(doc.id())?;
                    println!("Active doc is now {}", fmt_short(doc.id().as_bytes()));
                }
            }
            Self::Join { ticket, switch } => {
                if switch && !env.is_console() {
                    bail!("The --switch flag is only supported within the Iroh console.");
                }

                let doc = iroh.docs.import(ticket).await?;
                println!("{}", doc.id());

                if switch {
                    env.set_doc(doc.id())?;
                    println!("Active doc is now {}", fmt_short(doc.id().as_bytes()));
                }
            }
            Self::List => {
                let mut stream = iroh.docs.list().await?;
                while let Some(id) = stream.try_next().await? {
                    println!("{}", id)
                }
            }
            Self::Share { doc, mode } => {
                let doc = get_doc(iroh, env, doc).await?;
                let ticket = doc.share(mode).await?;
                println!("{}", ticket);
            }
            Self::Set {
                doc,
                author,
                key,
                value,
            } => {
                let doc = get_doc(iroh, env, doc).await?;
                let author = env.author(author)?;
                let key = key.as_bytes().to_vec();
                let value = value.as_bytes().to_vec();
                let hash = doc.set_bytes(author, key, value).await?;
                println!("{}", hash);
            }
            Self::Del {
                doc,
                author,
                prefix,
            } => {
                let doc = get_doc(iroh, env, doc).await?;
                let author = env.author(author)?;
                let prompt =
                    format!("Deleting all entries whose key starts with {prefix}. Continue?");
                if Confirm::new()
                    .with_prompt(prompt)
                    .interact()
                    .unwrap_or(false)
                {
                    let key = prefix.as_bytes().to_vec();
                    let removed = doc.del(author, key).await?;
                    println!("Deleted {removed} entries.");
                    println!(
                        "Inserted an empty entry for author {} with key {prefix}.",
                        fmt_short(author)
                    );
                } else {
                    println!("Aborted.")
                }
            }
            Self::Get {
                doc,
                key,
                prefix,
                author,
                mode,
            } => {
                let doc = get_doc(iroh, env, doc).await?;
                let key = key.as_bytes().to_vec();
                let filter = match (author, prefix) {
                    (None, false) => GetFilter::Key(key),
                    (None, true) => GetFilter::Prefix(key),
                    (Some(author), true) => GetFilter::AuthorAndPrefix(author, key),
                    (Some(author), false) => {
                        // Special case: Author and key, this means single entry.
                        let entry = doc
                            .get_one(author, key)
                            .await?
                            .ok_or_else(|| anyhow!("Entry not found"))?;
                        println!("{}", fmt_entry(&doc, &entry, mode).await);
                        return Ok(());
                    }
                };

                let mut stream = doc.get_many(filter).await?;
                while let Some(entry) = stream.try_next().await? {
                    println!("{}", fmt_entry(&doc, &entry, mode).await);
                }
            }
            Self::Keys {
                doc,
                prefix,
                author,
                mode,
            } => {
                let doc = get_doc(iroh, env, doc).await?;
                let filter = GetFilter::author_prefix(author, prefix);

                let mut stream = doc.get_many(filter).await?;
                while let Some(entry) = stream.try_next().await? {
                    println!("{}", fmt_entry(&doc, &entry, mode).await);
                }
            }
            Self::Leave { doc } => {
                let doc = get_doc(iroh, env, doc).await?;
                doc.leave().await?;
                println!("Doc {} is now inactive", fmt_short(doc.id()));
            }
            Self::Import {
                doc,
                author,
                prefix,
                path,
                in_place,
            } => {
                let doc = get_doc(iroh, env, doc).await?;
                let author = env.author(author)?;
                let mut prefix = prefix.unwrap_or_else(|| String::from(""));

                if prefix.ends_with('/') {
                    prefix.pop();
                }
                let root = canonicalize_path(&path)?.canonicalize()?;
                let tag = tag_from_file_name(&root)?;
                let stream = iroh
                    .blobs
                    .add_from_path(
                        root.clone(),
                        in_place,
                        SetTagOption::Named(tag.clone()),
                        WrapOption::NoWrap,
                    )
                    .await?;
                let (updates, progress) = doc.set_hash_streaming(author).await?;
                let root_prefix = match root.parent() {
                    Some(p) => p.to_path_buf(),
                    None => PathBuf::new(),
                };
                let (entries, size) =
                    import_coordinator(doc.id(), root_prefix, prefix, stream, updates, progress)
                        .await?;
                println!("Imported {} entries totaling {}", entries, HumanBytes(size));
            }
            Self::Export { doc, key, path } => {
                let doc = get_doc(iroh, env, doc).await?;
                let key_str = key.clone();
                let key = key.as_bytes().to_vec();
                let filter = GetFilter::Key(key);
                let path: PathBuf = canonicalize_path(&path)?;

                let mut stream = doc.get_many(filter).await?;
                while let Some(entry) = stream.try_next().await? {
                    match doc.read_to_bytes(&entry).await {
                        Ok(content) => {
                            if let Some(dir) = path.parent() {
                                if let Err(err) = std::fs::create_dir_all(dir) {
                                    println!(
                                        "<unable to create directory for {}: {err}>",
                                        path.display()
                                    );
                                }
                            };
                            if let Err(err) = std::fs::write(path.clone(), content) {
                                println!("<unable to write to file {}: {err}>", path.display())
                            } else {
                                println!("wrote '{key_str}' to {}", path.display());
                            }
                        }
                        Err(err) => println!("<failed to get content: {err}>"),
                    }
                }
            }
            Self::Watch { doc } => {
                let doc = get_doc(iroh, env, doc).await?;
                let mut stream = doc.subscribe().await?;
                while let Some(event) = stream.next().await {
                    let event = event?;
                    match event {
                        LiveEvent::InsertLocal { entry } => {
                            println!(
                                "local change:  {}",
                                fmt_entry(&doc, &entry, DisplayContentMode::Auto).await
                            )
                        }
                        LiveEvent::InsertRemote {
                            entry,
                            from,
                            content_status,
                        } => {
                            let content = match content_status {
                                iroh_sync::ContentStatus::Complete => {
                                    fmt_entry(&doc, &entry, DisplayContentMode::Auto).await
                                }
                                iroh_sync::ContentStatus::Incomplete => {
                                    let (Ok(content) | Err(content)) =
                                        fmt_content(&doc, &entry, DisplayContentMode::Hash).await;
                                    format!("<incomplete: {} ({})>", content, human_len(&entry))
                                }
                                iroh_sync::ContentStatus::Missing => {
                                    let (Ok(content) | Err(content)) =
                                        fmt_content(&doc, &entry, DisplayContentMode::Hash).await;
                                    format!("<missing: {} ({})>", content, human_len(&entry))
                                }
                            };
                            println!(
                                "remote change via @{}: {}",
                                fmt_short(from.as_bytes()),
                                content
                            )
                        }
                        LiveEvent::ContentReady { hash } => {
                            println!("content ready: {}", fmt_short(hash.as_bytes()))
                        }
                        LiveEvent::SyncFinished(event) => {
                            let origin = match event.origin {
                                Origin::Accept => "they initiated",
                                Origin::Connect(_) => "we initiated",
                            };
                            match event.result {
                                Ok(_) => println!(
                                    "synced doc {} with peer {} ({origin})",
                                    fmt_short(event.namespace),
                                    fmt_short(event.peer)
                                ),
                                Err(err) => println!(
                                    "failed to sync doc {} with peer {} ({origin}): {err}",
                                    fmt_short(event.namespace),
                                    fmt_short(event.peer)
                                ),
                            }
                        }
                        LiveEvent::NeighborUp(peer) => {
                            println!("neighbor peer up: {peer:?}");
                        }
                        LiveEvent::NeighborDown(peer) => {
                            println!("neighbor peer down: {peer:?}");
                        }
                        LiveEvent::Closed => println!("document closed"),
                    }
                }
            }
            Self::Drop { doc } => {
                let doc = get_doc(iroh, env, doc).await?;
                println!(
                    "Deleting a document will permanently remove the document secret key, all document entries, \n\
                    and all content blobs which are not referenced from other docs or tags."
                );
                let prompt = format!("Delete document {}?", fmt_short(doc.id()));
                if Confirm::new()
                    .with_prompt(prompt)
                    .interact()
                    .unwrap_or(false)
                {
                    iroh.docs.drop_doc(doc.id()).await?;
                    println!("Doc {} has been deleted.", fmt_short(doc.id()));
                } else {
                    println!("Aborted.")
                }
            }
        }
        Ok(())
    }
}

async fn get_doc(iroh: &Iroh, env: &ConsoleEnv, id: Option<NamespaceId>) -> anyhow::Result<Doc> {
    iroh.docs
        .get(env.doc(id)?)
        .await?
        .context("Document not found")
}

impl AuthorCommands {
    pub async fn run(self, iroh: &Iroh, env: &ConsoleEnv) -> Result<()> {
        match self {
            Self::Switch { author } => {
                env.set_author(author)?;
                println!("Active author is now {}", fmt_short(author.as_bytes()));
            }
            Self::List => {
                let mut stream = iroh.authors.list().await?;
                while let Some(author_id) = stream.try_next().await? {
                    println!("{}", author_id);
                }
            }
            Self::New { switch } => {
                if switch && !env.is_console() {
                    bail!("The --switch flag is only supported within the Iroh console.");
                }

                let author_id = iroh.authors.create().await?;
                println!("{}", author_id);

                if switch {
                    env.set_author(author_id)?;
                    println!("Active author is now {}", fmt_short(author_id.as_bytes()));
                }
            }
        }
        Ok(())
    }
}

/// Format the content. If an error occurs it's returned in a formatted, friendly way.
async fn fmt_content(doc: &Doc, entry: &Entry, mode: DisplayContentMode) -> Result<String, String> {
    let read_failed = |err: anyhow::Error| format!("<failed to get content: {err}>");
    let encode_hex = |err: std::string::FromUtf8Error| format!("0x{}", hex::encode(err.as_bytes()));
    let as_utf8 = |buf: Vec<u8>| String::from_utf8(buf).map(|repr| format!("\"{repr}\""));

    match mode {
        DisplayContentMode::Auto => {
            if entry.record().content_len() < MAX_DISPLAY_CONTENT_LEN {
                // small content: read fully as UTF-8
                let bytes = doc.read_to_bytes(entry).await.map_err(read_failed)?;
                Ok(as_utf8(bytes.into()).unwrap_or_else(encode_hex))
            } else {
                // large content: read just the first part as UTF-8
                let mut blob_reader = doc.read(entry).await.map_err(read_failed)?;
                let mut buf = Vec::with_capacity(MAX_DISPLAY_CONTENT_LEN as usize + 5);

                blob_reader
                    .read_buf(&mut buf)
                    .await
                    .map_err(|io_err| read_failed(io_err.into()))?;
                let mut repr = as_utf8(buf).unwrap_or_else(encode_hex);
                // let users know this is not shown in full
                repr.push_str("...");
                Ok(repr)
            }
        }
        DisplayContentMode::Content => {
            // read fully as UTF-8
            let bytes = doc.read_to_bytes(entry).await.map_err(read_failed)?;
            Ok(as_utf8(bytes.into()).unwrap_or_else(encode_hex))
        }
        DisplayContentMode::Hash => {
            let hash = entry.record().content_hash();
            Ok(fmt_short(hash.as_bytes()))
        }
    }
}

/// Human bytes for the contents of this entry.
fn human_len(entry: &Entry) -> HumanBytes {
    HumanBytes(entry.record().content_len())
}

#[must_use = "this won't be printed, you need to print it yourself"]
async fn fmt_entry(doc: &Doc, entry: &Entry, mode: DisplayContentMode) -> String {
    let id = entry.id();
    let key = std::str::from_utf8(id.key()).unwrap_or("<bad key>").bold();
    let author = fmt_short(id.author());
    let (Ok(content) | Err(content)) = fmt_content(doc, entry, mode).await;
    let len = human_len(entry);
    format!("@{author}: {key} = {content} ({len})")
}

/// Format the first 5 bytes of a byte string in bas32
pub fn fmt_short(hash: impl AsRef<[u8]>) -> String {
    let mut text = data_encoding::BASE32_NOPAD.encode(&hash.as_ref()[..5]);
    text.make_ascii_lowercase();
    format!("{}…", &text)
}

fn canonicalize_path(path: &str) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(shellexpand::tilde(&path).to_string());
    Ok(path)
}

fn tag_from_file_name(path: &Path) -> anyhow::Result<Tag> {
    match path.file_name() {
        Some(name) => name
            .to_os_string()
            .into_string()
            .map(|t| t.into())
            .map_err(|e| anyhow!("{e:?} contains invalid Unicode")),
        None => bail!("the given `path` does not have a proper directory or file name"),
    }
}

pub async fn import_coordinator(
    doc_id: NamespaceId,
    root: PathBuf,
    prefix: String,
    mut blob_add_progress: impl Stream<Item = Result<AddProgress>> + Send + Unpin + 'static,
    mut doc_set_updates: UpdateSink<
        ProviderService,
        QuinnConnection<ProviderResponse, ProviderRequest>,
        DocSetStreamUpdate,
    >,
    mut doc_set_progress: impl Stream<Item = Result<DocSetProgress>> + Unpin,
) -> Result<(u64, u64)> {
    let mut mp = ImportProgressState::new(doc_id);
    let mut task_mp = mp.clone();
    // new task to iterate through the stream of added files
    // and send them through the `doc_set_updates` `UpdateSink`
    // to get added to the doc
    let add_progress_task = tokio::spawn(async move {
        let mut collections = BTreeMap::<u64, (String, u64, Option<Hash>)>::new();
        while let Some(item) = blob_add_progress.next().await {
            match item? {
                AddProgress::Found { name, id, size } => {
                    tracing::trace!("Found({id},{name},{size})");
                    task_mp.found(name.clone(), id, size).await;
                    collections.insert(id, (name, size, None));
                }
                AddProgress::Progress { id, offset } => {
                    tracing::trace!("Progress({id}, {offset})");
                    task_mp.add_progress(id, offset).await;
                }
                AddProgress::Done { hash, id } => {
                    tracing::trace!("Done({id},{hash:?})");
                    task_mp.adding_to_doc(id).await;
                    match collections.get_mut(&id) {
                        Some((path_str, size, ref mut h)) => {
                            *h = Some(hash);
                            let key = match key_from_path_str(
                                root.clone(),
                                prefix.clone(),
                                path_str.clone(),
                            ) {
                                Ok(k) => k,
                                Err(e) => {
                                    doc_set_updates
                                        .send(DocSetStreamUpdate::Abort)
                                        .await
                                        .expect("receiver dropped");
                                    anyhow::bail!("issue creating key for entry {hash:?}: {e}");
                                }
                            };
                            // send update to doc
                            doc_set_updates
                                .send(DocSetStreamUpdate::Entry {
                                    id,
                                    key,
                                    hash,
                                    size: *size,
                                })
                                .await
                                .expect("receiver dropped");
                        }
                        None => {
                            anyhow::bail!("Got Done for unknown collection id {id}");
                        }
                    }
                }
                AddProgress::AllDone { hash, .. } => {
                    tracing::trace!("AddProgress::AllDone({hash:?})");
                    break;
                }
                AddProgress::Abort(e) => {
                    task_mp.error().await;
                    doc_set_updates
                        .send(DocSetStreamUpdate::Abort)
                        .await
                        .expect("receiver dropped");
                    anyhow::bail!("Error while adding data: {e}");
                }
            }
        }
        Ok(())
    });
    let mut entries = 0;
    let mut total_size = 0;
    while let Some(res) = doc_set_progress.next().await {
        match res? {
            DocSetProgress::Done { id, size, .. } => {
                mp.done(id).await;
                entries += 1;
                total_size += size;
            }
            DocSetProgress::AllDone => {
                mp.all_done().await;
                add_progress_task.abort();
                return Ok((entries, total_size));
            }
            DocSetProgress::Abort(e) => {
                mp.error().await;
                add_progress_task.abort();
                anyhow::bail!("Error while adding entry to doc: {e}")
            }
        }
    }
    unreachable!();
}

fn key_from_path_str(root: PathBuf, prefix: String, path_str: String) -> Result<Vec<u8>> {
    let suffix = PathBuf::from(path_str)
        .strip_prefix(root)?
        .to_str()
        .map(|p| p.as_bytes())
        .ok_or(anyhow!("could not convert path to bytes"))?
        .to_vec();
    let mut key = prefix.into_bytes().to_vec();
    key.extend(suffix);
    Ok(key)
}

#[derive(Debug, Clone)]
struct ImportProgressState {
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: flume::Sender<ImportProgressEvent>,
}

enum ImportProgressEvent {
    Found { name: String, id: u64, size: u64 },
    AddProgress { id: u64, progress: u64 },
    AddingToDoc { id: u64 },
    Done { id: u64 },
    AllDone,
    Error,
}

impl ImportProgressState {
    fn new(doc_id: NamespaceId) -> Self {
        let (sender, recv) = flume::bounded(32);
        let task = tokio::spawn(async move {
            let mp = MultiProgress::new();
            let mut pbs = HashMap::new();
            while let Ok(event) = recv.recv() {
                match event {
                    ImportProgressEvent::Found { name, id, size } => {
                        let pb = mp.add(ProgressBar::new(size));
                        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {msg} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})").unwrap()
            .progress_chars("=>-"));
                        pb.set_message(name);
                        pb.set_length(size);
                        pb.set_position(0);
                        pb.enable_steady_tick(Duration::from_millis(500));
                        pbs.insert(id, pb);
                    }
                    ImportProgressEvent::AddProgress { id, progress } => {
                        if let Some(pb) = pbs.get_mut(&id) {
                            pb.set_position(progress);
                        }
                    }
                    ImportProgressEvent::AddingToDoc { id } => {
                        if let Some(pb) = pbs.get_mut(&id) {
                            pb.finish_with_message(format!(
                                "Adding to doc {}...",
                                fmt_short(doc_id.as_bytes())
                            ));
                        }
                    }
                    ImportProgressEvent::Done { id } => {
                        if let Some(pb) = pbs.remove(&id) {
                            pb.finish_and_clear();
                            mp.remove(&pb);
                        }
                    }
                    ImportProgressEvent::AllDone | ImportProgressEvent::Error => {
                        mp.clear().ok();
                        return;
                    }
                }
            }
        });
        Self {
            task: Arc::new(Mutex::new(Some(task))),
            sender: sender.clone(),
        }
    }

    async fn found(&mut self, name: String, id: u64, size: u64) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::Found { name, id, size })
                .await
                .expect("checked");
        }
    }

    async fn add_progress(&mut self, id: u64, progress: u64) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::AddProgress { id, progress })
                .await
                .expect("checked");
        }
    }

    async fn adding_to_doc(&mut self, id: u64) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::AddingToDoc { id })
                .await
                .expect("checked");
        }
    }

    async fn done(&mut self, id: u64) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::Done { id })
                .await
                .expect("checked");
        }
    }

    async fn all_done(self) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::AllDone)
                .await
                .expect("checked");
        }
        self.shutdown();
    }

    async fn error(self) {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(ImportProgressEvent::Error)
                .await
                .expect("receiever dropped");
        }
        self.shutdown();
    }

    fn shutdown(self) {
        match self.task.lock() {
            Err(e) => {
                tracing::error!("error cleaning up import progress bar: {e}")
            }
            Ok(mut task) => {
                if let Some(task) = task.take() {
                    task.abort();
                }
            }
        }
    }
}
