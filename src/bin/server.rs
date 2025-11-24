//! Time-series database server binary
//!
//! This is a placeholder for the full server implementation.

use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Gorilla TSDB Server starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // TODO: Initialize database
    // TODO: Start HTTP/gRPC server
    // TODO: Handle graceful shutdown

    println!("Server implementation coming soon!");
    println!("To use the library, see examples/ directory");

    Ok(())
}
