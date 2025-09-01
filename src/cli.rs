use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    /// Duration(in seconds) to aggregate metrics before starting a new interval.
    pub flush_interval: u64,

    #[arg(long, default_value_t = 1212)]
    /// Port to listen for metrics over UDP.
    pub udp_port: u16,

    #[arg(long, default_value_t = 1213)]
    /// Port to listen for Arrow Flight data over gRPC.
    pub flight_port: u16,

    #[arg(long, default_value_t = 1214)]
    /// Port to host Http server on.
    pub http_port: u16,

    #[arg(long)]
    /// Path to store metrics data files.
    pub data_dir: PathBuf,
}
