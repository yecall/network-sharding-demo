mod params;
mod network;
mod parse;
mod bootnodes_router;

use futures::future::Future;
use crate::params::{match_args, RunCmd};
use crate::params::CoreParams::{Run, BootNodesRouter};
use crate::bootnodes_router::run_bootnodes_router;
use crate::bootnodes_router::get_bootnodes_router_conf;
use primitive_types::H256;
use libp2p::identity::{Keypair, secp256k1::SecretKey};
use std::{str::FromStr};

#[macro_use]
extern crate jsonrpc_client_core;

#[derive(Clone)]
pub struct VersionInfo {
    /// Implemtation name.
    pub name: &'static str,
    /// Executable file name.
    pub executable_name: &'static str,
    /// Executable file author.
    pub author: &'static str,
}

fn main() {
    let version = VersionInfo {
        name: "Network sharding demo",
        executable_name: "network-sharding-demo",
        author: "contact@yeefoundation.com",
    };

    run(version);
}

fn run(version: VersionInfo) {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
//    env_logger::init();

    let args = match_args(version.clone());

    match args {
        Run(run_cmd) => run_main(run_cmd),
        BootNodesRouter(bootnodes_router_cmd) => run_bootnodes_router(bootnodes_router_cmd, version.clone()),
    }
}

fn run_main(cmd: RunCmd) {
    let (signal, exit) = exit_future::signal();

    let cmd_clone = cmd.clone();

    let local_identity = get_local_identity(cmd.node_key);

    let bootnodes_router_conf = get_bootnodes_router_conf(cmd.bootnodes_router);

    let network = network::Network::new(
        cmd_clone,
        local_identity.clone(),
        bootnodes_router_conf);

    network.run();

    exit.wait().unwrap();

    signal.fire();
}

fn get_local_identity(node_key: Option<String>) -> Keypair {

    let node_key: Option<SecretKey> = match node_key.map(|k| {
        H256::from_str(k.as_str()).map_err(|_err| "").and_then(|bytes| SecretKey::from_bytes(bytes).map_err(|_err| ""))
    }) {
        Some(Ok(r)) => Some(r),
        Some(Err(_e)) => None,
        None => None,
    };
    let local_identity = node_key.map_or(Keypair::generate_secp256k1(), |k| { Keypair::Secp256k1(k.into()) });

    local_identity
}

