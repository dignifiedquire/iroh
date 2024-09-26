//! Define the net subcommands.

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use comfy_table::{presets::NOTHING, Cell, Table};
use futures_lite::{Stream, StreamExt};
use human_time::ToHumanTimeString;
use iroh::{
    client::Iroh,
    net::{
        endpoint::{DirectAddrInfo, RemoteInfo},
        relay::RelayUrl,
        {NodeAddr, NodeId},
    },
};
use std::{net::SocketAddr, time::Duration};

/// Commands to manage the iroh network.
#[derive(Subcommand, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NetCommands {
    /// Get information about the different remote nodes.
    RemoteList,
    /// Get information about a particular remote node.
    Remote { node_id: NodeId },
    /// Get the node addr of this node.
    NodeAddr,
    /// Add this node addr to the known nodes.
    AddNodeAddr {
        node_id: NodeId,
        relay: Option<RelayUrl>,
        addresses: Vec<SocketAddr>,
    },
    /// Get the relay server we are connected to.
    HomeRelay,
}

impl NetCommands {
    /// Runs the net command given the iroh client.
    pub async fn run(self, iroh: &Iroh) -> Result<()> {
        match self {
            Self::RemoteList => {
                let connections = iroh.net().remote_info_iter().await?;
                let timestamp = time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc2822)
                    .unwrap_or_else(|_| String::from("failed to get current time"));

                println!(
                    " {}: {}\n\n{}",
                    "current time".bold(),
                    timestamp,
                    fmt_remote_infos(connections).await
                );
            }
            Self::Remote { node_id } => {
                let info = iroh.net().remote_info(node_id).await?;
                match info {
                    Some(info) => println!("{}", fmt_info(info)),
                    None => println!("Not Found"),
                }
            }
            Self::NodeAddr => {
                let addr = iroh.net().node_addr().await?;
                println!("Node ID: {}", addr.node_id);
                let relay = addr
                    .info
                    .relay_url
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Not Available".to_string());
                println!("Home Relay: {}", relay);
                println!("Direct Addresses ({}):", addr.info.direct_addresses.len());
                for da in &addr.info.direct_addresses {
                    println!(" {}", da);
                }
            }
            Self::AddNodeAddr {
                node_id,
                relay,
                addresses,
            } => {
                let mut addr = NodeAddr::new(node_id).with_direct_addresses(addresses);
                if let Some(relay) = relay {
                    addr = addr.with_relay_url(relay);
                }
                iroh.net().add_node_addr(addr).await?;
            }
            Self::HomeRelay => {
                let relay = iroh.net().home_relay().await?;
                let relay = relay
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Not Available".to_string());
                println!("Home Relay: {}", relay);
            }
        }
        Ok(())
    }
}

/// Formats the remote information into a `Table`.
async fn fmt_remote_infos(
    mut infos: impl Stream<Item = Result<RemoteInfo, anyhow::Error>> + Unpin,
) -> String {
    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(
        [
            "node id",
            "relay",
            "conn type",
            "latency",
            "last used",
            "last source",
        ]
        .into_iter()
        .map(bold_cell),
    );
    while let Some(Ok(info)) = infos.next().await {
        let node_id: Cell = info.node_id.to_string().into();
        let relay_url = info
            .relay_url
            .map_or(String::new(), |url_info| url_info.relay_url.to_string())
            .into();
        let conn_type = info.conn_type.to_string().into();
        let latency = match info.latency {
            Some(latency) => latency.to_human_time_string(),
            None => String::from("unknown"),
        }
        .into();
        let last_used = info
            .last_used
            .map(fmt_how_long_ago)
            .map(Cell::new)
            .unwrap_or_else(never);
        // let last_source = info
        //     .sources
        //     .pop()
        //     .map(|sources| sources.0.to_string())
        //     .map(Cell::new)
        //     .unwrap_or_else(|| Cell::new("none"));
        table.add_row([
            node_id, relay_url, conn_type, latency, last_used,
            // last_source,
        ]);
    }
    table.to_string()
}

/// Formats the remote information into a `String`.
fn fmt_info(info: RemoteInfo) -> String {
    let RemoteInfo {
        node_id,
        relay_url,
        addrs,
        conn_type,
        latency,
        last_used,
        // sources,
    } = info;
    let timestamp = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc2822)
        .unwrap_or_else(|_| String::from("failed to get current time"));
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.add_row([bold_cell("current time"), timestamp.into()]);
    table.add_row([bold_cell("node id"), node_id.to_string().into()]);
    let relay_url = relay_url
        .map(|r| r.relay_url.to_string())
        .unwrap_or_else(|| String::from("unknown"));
    table.add_row([bold_cell("relay url"), relay_url.into()]);
    table.add_row([bold_cell("connection type"), conn_type.to_string().into()]);
    table.add_row([bold_cell("latency"), fmt_latency(latency).into()]);
    table.add_row([
        bold_cell("last used"),
        last_used
            .map(fmt_how_long_ago)
            .map(Cell::new)
            .unwrap_or_else(never),
    ]);
    table.add_row([bold_cell("known addresses"), addrs.len().into()]);

    let general_info = table.to_string();

    let addrs_info = fmt_addrs(addrs);

    // let source_info = fmt_sources(sources);
    // format!("{general_info}\n\n{addrs_info}\n\n{source_info}",)
    format!("{general_info}\n\n{addrs_info}")
}

/// Formats the [`DirectAddrInfo`] into a [`Table`].
fn direct_addr_row(info: DirectAddrInfo) -> comfy_table::Row {
    let DirectAddrInfo {
        addr,
        latency,
        last_control,
        last_payload,
        last_alive,
        ..
    } = info;

    let last_control = match last_control {
        None => never(),
        Some((how_long_ago, kind)) => {
            format!("{kind} ( {} )", fmt_how_long_ago(how_long_ago)).into()
        }
    };
    let last_payload = last_payload
        .map(fmt_how_long_ago)
        .map(Cell::new)
        .unwrap_or_else(never);

    let last_alive = last_alive
        .map(fmt_how_long_ago)
        .map(Cell::new)
        .unwrap_or_else(never);

    [
        addr.into(),
        fmt_latency(latency).into(),
        last_control,
        last_payload,
        last_alive,
    ]
    .into()
}

/// Formats a collection o [`DirectAddrInfo`] into a [`Table`].
fn fmt_addrs(addrs: Vec<DirectAddrInfo>) -> comfy_table::Table {
    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(
        vec!["addr", "latency", "last control", "last data", "last alive"]
            .into_iter()
            .map(bold_cell),
    );
    table.add_rows(addrs.into_iter().map(direct_addr_row));
    table
}

// /// Formats a list of [`Source`]s into a [`Table`].
// fn fmt_sources(sources: Vec<(Source, Duration)>) -> comfy_table::Table {
//     let mut table = Table::new();
//     table
//         .load_preset(NOTHING)
//         .set_header(vec!["source", "added"].into_iter().map(bold_cell));
//     // `rev` because sources recently added sources are at the end of the list
//     table.add_rows(sources.into_iter().rev().map(source_row));
//     table
// }

// /// Formats a [`Source`] into a [`Row`]
// fn source_row(source: (Source, Duration)) -> comfy_table::Row {
//     let (source, duration) = source;

//     let duration = Cell::new(format!("{} ago", fmt_how_long_ago(duration)));

//     [Cell::new(source.to_string()), duration].into()
// }

/// Creates a cell with the dimmed text "never".
fn never() -> Cell {
    Cell::new("never").add_attribute(comfy_table::Attribute::Dim)
}

/// Formats a [`Duration`] into a human-readable `String`.
fn fmt_how_long_ago(duration: Duration) -> String {
    duration
        .to_human_time_string()
        .split_once(',')
        .map(|(first, _rest)| first)
        .unwrap_or("-")
        .to_string()
}

/// Formats the latency into a human-readable `String`.
fn fmt_latency(latency: Option<Duration>) -> String {
    match latency {
        Some(latency) => latency.to_human_time_string(),
        None => String::from("unknown"),
    }
}

/// Creates a bold cell with the given text.
fn bold_cell(s: &str) -> Cell {
    Cell::new(s).add_attribute(comfy_table::Attribute::Bold)
}
