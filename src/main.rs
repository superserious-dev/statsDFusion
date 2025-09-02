use anyhow::Result;
use clap::Parser as _;
use log::info;
use statsdfusion::{
    cli, http_server, metrics_store,
    startup::{StartupConfig, start_services},
    udp_server,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("Starting statsDFusion...");

    // Parse CLI args
    let args = cli::Cli::parse();

    // Spin up services
    let mut udp_server =
        udp_server::UdpServer::new(args.udp_port, args.flight_port, args.flush_interval);
    let mut metrics_store = metrics_store::MetricsStore::new(args.data_dir, args.flight_port);
    let mut http_server = http_server::HttpServer::new(args.http_port, args.flight_port);
    let running_services = start_services(
        &mut metrics_store,
        &mut udp_server,
        &mut http_server,
        StartupConfig::default(),
    )
    .await?;

    // Block until services shut down
    running_services.wait_until_shutdown().await?;

    info!("Stopped statsDFusion.");

    Ok(())
}
