use structopt::StructOpt;
use std::path::{PathBuf, Path};
use app_dirs::{self, AppInfo, AppDataType};
use crate::VersionInfo;
use clap::{App, SubCommand, AppSettings};

pub const DEFAULT_PORT : u16 = 60001;
pub const DEFAULT_BOOTNODES_ROUTER_PORT : u16 = 50001;


#[derive(Debug, Clone)]
pub enum CoreParams {
    Run(RunCmd),
    BootNodesRouter(BootNodesRouterCmd),
}

#[derive(Debug, StructOpt, Clone)]
pub struct RunCmd {
    #[structopt(long = "bootnodes", value_name = "URL")]
    pub bootnodes: Vec<String>,

    #[structopt(long = "bootnodes-router", value_name = "URL")]
    pub bootnodes_router: Vec<String>,

    #[structopt(long = "node-key", value_name = "KEY")]
    pub node_key: Option<String>,

    #[structopt(long = "shard-num", value_name = "SHARD_NUM")]
    pub shard_num: u16,

    #[allow(missing_docs)]
    #[structopt(flatten)]
    pub shared_params: SharedParams,

}
//
#[derive(Debug, StructOpt, Clone)]
pub struct BootNodesRouterCmd{

    #[allow(missing_docs)]
    #[structopt(flatten)]
    pub shared_params: SharedParams,
}

#[derive(Debug, StructOpt, Clone)]
pub struct SharedParams {

    #[structopt(long = "port", value_name = "PORT")]
    pub port: Option<u16>,

    /// Specify custom base path.
    #[structopt(long = "base-path", short = "d", value_name = "PATH", parse(from_os_str))]
    pub base_path: Option<PathBuf>,
}

impl StructOpt for CoreParams {
    fn clap<'a, 'b>() -> App<'a, 'b> {
        RunCmd::augment_clap(App::new("app"))
            .subcommand(
                BootNodesRouterCmd::augment_clap(SubCommand::with_name("bootnodes-router"))
                    .about("Run bootnodes router.")
            )
    }

    fn from_clap(matches: &::structopt::clap::ArgMatches) -> Self {
        match matches.subcommand() {
            ("bootnodes-router", Some(matches)) =>
                CoreParams::BootNodesRouter(BootNodesRouterCmd::from_clap(matches)),
            (_, None) => CoreParams::Run(RunCmd::from_clap(matches)),
            _ => CoreParams::Run(RunCmd::from_clap(matches)),
        }
    }
}

pub fn match_args(version: VersionInfo) -> CoreParams {
    let matches = CoreParams::clap()
        .name(version.executable_name)
        .author(version.author)
        .setting(AppSettings::GlobalVersion)
        .setting(AppSettings::ArgsNegateSubcommands)
        .setting(AppSettings::SubcommandsNegateReqs)
        .get_matches_from(::std::env::args());

    let cli_args = CoreParams::from_clap(&matches);

    cli_args
}

pub fn base_path(base_path: Option<PathBuf>, version: VersionInfo) -> PathBuf {
    base_path.clone()
        .unwrap_or_else(||
            app_dirs::get_app_root(
                AppDataType::UserData,
                &AppInfo {
                    name: version.executable_name,
                    author: version.author,
                },
            ).expect("app directories exist on all supported platforms; qed")
        )
}

pub fn conf_path(base_path: &Path) -> PathBuf {
    let mut path = base_path.to_owned();
    path.push("conf");
    path
}