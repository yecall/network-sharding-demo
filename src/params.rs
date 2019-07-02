
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "basic")]
pub struct RunCmd {

    #[structopt(long = "bootnodes", value_name = "URL")]
    pub bootnodes: Vec<String>,

    #[structopt(long = "node-key", value_name = "KEY")]
    pub node_key: Option<String>,

    #[structopt(long = "port", value_name = "PORT")]
    pub port: Option<u16>,

}