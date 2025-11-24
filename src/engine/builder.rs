//! Database builder with pluggable engines

use super::traits::{Compressor, IndexConfig, StorageConfig, StorageEngine, TimeIndex};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Database configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Data directory for storage
    pub data_dir: PathBuf,
    /// Redis connection URL
    pub redis_url: Option<String>,
    /// Maximum chunk size in bytes
    pub max_chunk_size: usize,
    /// Retention period in days
    pub retention_days: Option<u32>,
    /// Custom options
    pub custom_options: HashMap<String, String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/tsdb"),
            redis_url: None,
            max_chunk_size: 1024 * 1024, // 1MB
            retention_days: None,
            custom_options: HashMap::new(),
        }
    }
}

impl DatabaseConfig {
    /// Convert to storage config
    pub fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            base_path: Some(self.data_dir.to_string_lossy().to_string()),
            max_chunk_size: self.max_chunk_size,
            compression_enabled: true,
            retention_days: self.retention_days,
            custom_options: self.custom_options.clone(),
        }
    }

    /// Convert to index config
    pub fn index_config(&self) -> IndexConfig {
        IndexConfig {
            connection_string: self.redis_url.clone(),
            cache_size_mb: 128,
            max_series: 1_000_000,
            custom_options: self.custom_options.clone(),
        }
    }
}

/// Builder for configuring the time-series database with custom engines
pub struct TimeSeriesDBBuilder {
    compressor: Option<Arc<dyn Compressor + Send + Sync>>,
    storage: Option<Arc<dyn StorageEngine + Send + Sync>>,
    index: Option<Arc<dyn TimeIndex + Send + Sync>>,
    config: DatabaseConfig,
}

impl TimeSeriesDBBuilder {
    /// Create a new database builder
    pub fn new() -> Self {
        Self {
            compressor: None,
            storage: None,
            index: None,
            config: DatabaseConfig::default(),
        }
    }

    /// Set a custom compressor implementation
    pub fn with_compressor<C>(mut self, compressor: C) -> Self
    where
        C: Compressor + 'static,
    {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Set a custom storage engine implementation
    pub fn with_storage<S>(mut self, storage: S) -> Self
    where
        S: StorageEngine + 'static,
    {
        self.storage = Some(Arc::new(storage));
        self
    }

    /// Set a custom time index implementation
    pub fn with_index<I>(mut self, index: I) -> Self
    where
        I: TimeIndex + 'static,
    {
        self.index = Some(Arc::new(index));
        self
    }

    /// Set database configuration
    pub fn with_config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    /// Build the database with configured engines
    pub async fn build(self) -> Result<TimeSeriesDB> {
        // For now, we'll require all engines to be provided
        // Later we'll add default implementations

        let compressor = self
            .compressor
            .ok_or_else(|| Error::Configuration("No compressor configured".to_string()))?;

        let storage = self
            .storage
            .ok_or_else(|| Error::Configuration("No storage engine configured".to_string()))?;

        let index = self
            .index
            .ok_or_else(|| Error::Configuration("No index configured".to_string()))?;

        // Initialize engines
        storage
            .initialize(self.config.storage_config())
            .await
            .map_err(Error::Storage)?;

        index
            .initialize(self.config.index_config())
            .await
            .map_err(Error::Index)?;

        Ok(TimeSeriesDB {
            compressor,
            storage,
            index,
            config: self.config,
        })
    }
}

impl Default for TimeSeriesDBBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Main database instance with pluggable engines
pub struct TimeSeriesDB {
    compressor: Arc<dyn Compressor + Send + Sync>,
    storage: Arc<dyn StorageEngine + Send + Sync>,
    index: Arc<dyn TimeIndex + Send + Sync>,
    config: DatabaseConfig,
}

impl TimeSeriesDB {
    /// Get reference to the compressor
    pub fn compressor(&self) -> &Arc<dyn Compressor + Send + Sync> {
        &self.compressor
    }

    /// Get reference to the storage engine
    pub fn storage(&self) -> &Arc<dyn StorageEngine + Send + Sync> {
        &self.storage
    }

    /// Get reference to the time index
    pub fn index(&self) -> &Arc<dyn TimeIndex + Send + Sync> {
        &self.index
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Replace compressor at runtime
    pub fn set_compressor(&mut self, compressor: Arc<dyn Compressor + Send + Sync>) {
        self.compressor = compressor;
    }

    /// Replace storage engine at runtime (dangerous - ensure data migration)
    pub async fn set_storage(
        &mut self,
        storage: Arc<dyn StorageEngine + Send + Sync>,
    ) -> Result<()> {
        storage
            .initialize(self.config.storage_config())
            .await
            .map_err(Error::Storage)?;
        self.storage = storage;
        Ok(())
    }

    /// Replace index at runtime (requires rebuild)
    pub async fn set_index(&mut self, index: Arc<dyn TimeIndex + Send + Sync>) -> Result<()> {
        index
            .initialize(self.config.index_config())
            .await
            .map_err(Error::Index)?;
        index.rebuild().await.map_err(Error::Index)?;
        self.index = index;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.max_chunk_size, 1024 * 1024);
        assert!(config.redis_url.is_none());
    }

    #[test]
    fn test_builder_creation() {
        let builder = TimeSeriesDBBuilder::new();
        assert!(builder.compressor.is_none());
        assert!(builder.storage.is_none());
        assert!(builder.index.is_none());
    }
}
