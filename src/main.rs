
mod params;
mod network;
mod parse;
mod bootnodes_router;
use structopt::StructOpt;
use futures::future::Future;
use crate::params::{base_path, conf_path, CoreParams, match_args, RunCmd};
use crate::params::CoreParams::{Run, BootNodesRouter};
use crate::network::run_network;
use crate::bootnodes_router::run_bootnodes_router;

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
        author: "contact@yeefoundation.com"
    };

    run(version);
}

fn run(version: VersionInfo){

    env_logger::builder().filter_level(log::LevelFilter::Info).init();

    let args = match_args(version.clone());

    match args{
        Run(run_cmd) => run_main(run_cmd),
        BootNodesRouter(bootnodes_router_cmd) => run_bootnodes_router(bootnodes_router_cmd, version.clone()),
    }

}

fn run_main(cmd: RunCmd){
    let (signal, exit) = exit_future::signal();

    run_network(cmd);

    exit.wait().unwrap();

    signal.fire();
}

