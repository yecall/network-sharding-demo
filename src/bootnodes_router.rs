use crate::params::BootNodesRouterCmd;
use crate::VersionInfo;
use crate::params::{self, base_path, conf_path};
use futures::future::Future;
use log::{debug, info, trace, warn};
use std::fs::File;
use std::io::Read;
use serde_derive::{Deserialize, Serialize};
use std::path::PathBuf;
use std::collections::HashMap;
use jsonrpc_core::IoHandler;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use std::thread;
use futures::{prelude::*, future};
use jsonrpc_client_http::{self, HttpTransport, HttpHandle};

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct Shard {
    pub bootnodes: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct BootnodesRouterConf {
    pub shards: HashMap<String, Shard>,
}

fn get_bootnodes_router_conf(conf_path: &PathBuf) -> BootnodesRouterConf {
    let bootnodes_router_conf_path = conf_path.join("bootnodes-router.toml");

    info!("conf_path:{}", bootnodes_router_conf_path.to_string_lossy());

    let mut file = match File::open(&bootnodes_router_conf_path) {
        Ok(f) => f,
        Err(e) => panic!("no such file {} exception:{}", bootnodes_router_conf_path.to_string_lossy(), e)
    };

    let mut str_val = String::new();
    match file.read_to_string(&mut str_val) {
        Ok(s) => s,
        Err(e) => panic!("Error Reading file: {}", e)
    };

    let conf: BootnodesRouterConf = toml::from_str(&str_val).unwrap();

    conf
}

fn start_bootnodes_router(cmd: BootNodesRouterCmd, version: VersionInfo) {
    let thread = thread::Builder::new().name("bootnodes_router".to_string()).spawn(move || {
        start_http(cmd, version);
    });

    info!("Run bootnodes router successfully");
}

fn start_http(cmd: BootNodesRouterCmd, version: VersionInfo) {
    let conf_path = conf_path(&base_path(cmd.shared_params.base_path, version));

    let conf: BootnodesRouterConf = get_bootnodes_router_conf(&conf_path);

    info!("bootnodes router_conf={:?}", conf);

    let port = match cmd.shared_params.port {
        Some(port) => port,
        None => params::DEFAULT_BOOTNODES_ROUTER_PORT,
    };

    let io = rpc_handler(conf);
    let addr = format!("0.0.0.0:{}", port);
    let server = ServerBuilder::new(io).
        threads(4).start_http(&addr.parse().unwrap()).unwrap();

    info!("bootnodes router listen on: {}", addr);

    server.wait();
}

pub fn run_bootnodes_router(cmd: BootNodesRouterCmd, version: VersionInfo) {
    let (signal, exit) = exit_future::signal();

    start_bootnodes_router(cmd, version);

    exit.wait().unwrap();

    signal.fire();
}

fn rpc_handler(conf: BootnodesRouterConf) -> IoHandler<()> {
    let bootnodes_router_impl = BootnodesRouterImpl { conf };
    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(bootnodes_router_impl.to_delegate());
    io
}

#[rpc]
pub trait BootnodesRouter {
    #[rpc(name = "bootnodes")]
    fn bootnodes(&self) -> jsonrpc_core::Result<BootnodesRouterConf>;
}

struct BootnodesRouterImpl {
    conf: BootnodesRouterConf,
}

impl BootnodesRouter for BootnodesRouterImpl {
    fn bootnodes(&self) -> jsonrpc_core::Result<BootnodesRouterConf> {
        Ok(self.conf.clone())
    }
}

jsonrpc_client!(pub struct BootnodesRouterClient {
    pub fn bootnodes(&mut self) -> RpcRequest<BootnodesRouterConf>;
});

pub fn bootnodes_router_client(uri: String) -> BootnodesRouterClient<HttpHandle> {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport
        .handle(&uri)
        .unwrap();

    let a = BootnodesRouterClient::new(transport_handle);

    a
}