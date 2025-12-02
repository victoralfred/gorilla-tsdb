//! Gorilla TSDB HTTP Server Modules
//!
//! This binary provides a complete HTTP server for the Gorilla time-series database.

pub mod aggregation;
pub mod config;
pub mod handlers;
pub mod query_router;
pub mod types;

pub use config::ServerConfig;
pub use handlers::AppState;
