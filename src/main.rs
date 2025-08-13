use anyhow::Result;
use clap::Parser as _;
use log::info;
use statsdfusion::{
    cli, metrics_store,
    startup::{StartupConfig, start_services},
    udp_server,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("Starting statsDFusion...");

    // Parse CLI args
    let args = cli::Cli::parse();

    // Set up communication channels between services
    let (metrics_store_tx, metrics_store_rx): (
        UnboundedSender<metrics_store::Message>,
        UnboundedReceiver<metrics_store::Message>,
    ) = mpsc::unbounded_channel();

    // Spin up services
    let mut udp_server =
        udp_server::UdpServer::new(args.udp_port, args.flush_interval, metrics_store_tx);
    let mut metrics_store = metrics_store::MetricsStore::new(args.data_dir, metrics_store_rx);
    let running_services = start_services(
        &mut metrics_store,
        &mut udp_server,
        StartupConfig::default(),
    )
    .await?;

    // Block until services shut down
    running_services.wait_until_shutdown().await?;

    info!("Stopped statsDFusion.");

    Ok(())
}
