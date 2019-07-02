
mod params;
mod network;
mod parse;
use structopt::StructOpt;
use futures::future::Future;

fn main() {

    run();
}

fn run(){

    env_logger::init();

    let run_cmd: params::RunCmd = params::RunCmd::from_args();

    let (signal, exit) = exit_future::signal();

    network::run_network(run_cmd);

    exit.wait().unwrap();

    signal.fire();

}