//! Pluggable engine architecture for compression, storage, and indexing

pub mod traits;
pub mod builder;

pub use builder::{TimeSeriesDB, TimeSeriesDBBuilder, DatabaseConfig};
