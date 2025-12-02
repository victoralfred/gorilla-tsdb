//! Chunk Manager Service
//!
//! Monitors and manages the lifecycle of data chunks including:
//! - Monitoring active chunks for seal conditions
//! - Triggering chunk sealing based on size/age/point count
//! - Scheduling compression after sealing
//! - Handling chunk rotation and expiration

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::{interval, Instant};

use crate::storage::LocalDiskEngine;
use crate::types::SeriesId;

use super::framework::{RestartPolicy, Service, ServiceError, ServiceStatus};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the chunk manager service
#[derive(Debug, Clone)]
pub struct ChunkManagerConfig {
    /// Maximum size of a chunk before sealing (bytes)
    pub max_chunk_size: usize,

    /// Maximum age of a chunk before sealing
    pub max_chunk_age: Duration,

    /// Maximum number of points per chunk
    pub max_points_per_chunk: usize,

    /// Interval for checking seal conditions
    pub seal_check_interval: Duration,

    /// Enable automatic chunk rotation
    pub auto_rotation: bool,

    /// Retention period for old chunks (None = keep forever)
    pub retention_period: Option<Duration>,
}

impl Default for ChunkManagerConfig {
    fn default() -> Self {
        Self {
            max_chunk_size: 1024 * 1024,              // 1 MB
            max_chunk_age: Duration::from_secs(3600), // 1 hour
            max_points_per_chunk: 10_000,
            seal_check_interval: Duration::from_secs(10),
            auto_rotation: true,
            retention_period: None,
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics collected by the chunk manager
#[derive(Debug, Default, Clone)]
pub struct ChunkManagerStats {
    /// Number of chunks sealed
    pub chunks_sealed: u64,

    /// Number of chunks rotated
    pub chunks_rotated: u64,

    /// Number of chunks expired
    pub chunks_expired: u64,

    /// Total bytes sealed
    pub bytes_sealed: u64,

    /// Total points sealed
    pub points_sealed: u64,

    /// Number of seal check cycles
    pub check_cycles: u64,

    /// Last check time
    pub last_check: Option<Instant>,
}

// ============================================================================
// Chunk Manager Service
// ============================================================================

/// Background service for managing chunk lifecycle
///
/// The ChunkManager monitors active chunks across all series and:
/// - Seals chunks when they reach size/age/point limits
/// - Rotates chunks to ensure fresh data goes to new chunks
/// - Expires old chunks based on retention policy
/// - Collects statistics for monitoring
pub struct ChunkManager {
    /// Configuration
    config: ChunkManagerConfig,

    /// Storage engine reference (used for chunk operations)
    #[allow(dead_code)]
    storage: Arc<LocalDiskEngine>,

    /// Current service status
    status: RwLock<ServiceStatus>,

    /// Collected statistics
    stats: RwLock<ChunkManagerStats>,
}

impl ChunkManager {
    /// Create a new chunk manager service
    pub fn new(config: ChunkManagerConfig, storage: Arc<LocalDiskEngine>) -> Self {
        Self {
            config,
            storage,
            status: RwLock::new(ServiceStatus::Stopped),
            stats: RwLock::new(ChunkManagerStats::default()),
        }
    }

    /// Create with default configuration
    pub fn with_storage(storage: Arc<LocalDiskEngine>) -> Self {
        Self::new(ChunkManagerConfig::default(), storage)
    }

    /// Get current statistics
    pub fn stats(&self) -> ChunkManagerStats {
        self.stats.read().clone()
    }

    /// Run seal check cycle for all series
    async fn run_seal_check(&self) -> Result<(), ServiceError> {
        let start = Instant::now();

        // Get list of all series from storage
        let series_list = self.get_active_series().await?;

        let mut sealed_count = 0u64;
        let mut sealed_bytes = 0u64;
        let mut sealed_points = 0u64;

        for series_id in series_list {
            if let Some((bytes, points)) = self.check_and_seal_series(series_id).await? {
                sealed_count += 1;
                sealed_bytes += bytes;
                sealed_points += points;
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.chunks_sealed += sealed_count;
            stats.bytes_sealed += sealed_bytes;
            stats.points_sealed += sealed_points;
            stats.check_cycles += 1;
            stats.last_check = Some(start);
        }

        if sealed_count > 0 {
            tracing::debug!(
                sealed = sealed_count,
                bytes = sealed_bytes,
                points = sealed_points,
                duration_ms = start.elapsed().as_millis(),
                "Seal check completed"
            );
        }

        Ok(())
    }

    /// Get list of series with active chunks
    async fn get_active_series(&self) -> Result<Vec<SeriesId>, ServiceError> {
        // Get all series from storage engine
        // For now, return empty list - real implementation would query storage
        Ok(vec![])
    }

    /// Check if a series needs sealing and seal if necessary
    async fn check_and_seal_series(
        &self,
        _series_id: SeriesId,
    ) -> Result<Option<(u64, u64)>, ServiceError> {
        // Check if series has an active chunk that needs sealing
        // This would interact with the storage engine's active chunk management

        // For now, this is a placeholder - real implementation would:
        // 1. Get the active chunk for the series
        // 2. Check seal conditions (size, age, point count)
        // 3. Seal if conditions are met
        // 4. Return (bytes, points) if sealed

        Ok(None)
    }

    /// Run expiration check for old chunks
    async fn run_expiration_check(&self) -> Result<u64, ServiceError> {
        let retention = match self.config.retention_period {
            Some(r) => r,
            None => return Ok(0), // No retention policy
        };

        let _cutoff = Instant::now() - retention;
        let expired_count = 0u64;

        // Find and delete chunks older than retention period
        // This would query the storage engine for old chunks

        if expired_count > 0 {
            let mut stats = self.stats.write();
            stats.chunks_expired += expired_count;
        }

        Ok(expired_count)
    }
}

#[async_trait::async_trait]
impl Service for ChunkManager {
    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
        *self.status.write() = ServiceStatus::Running;
        tracing::debug!(
            interval_secs = self.config.seal_check_interval.as_secs(),
            "Chunk manager started"
        );

        let mut check_interval = interval(self.config.seal_check_interval);

        loop {
            tokio::select! {
                // Shutdown signal received
                result = shutdown.recv() => {
                    match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!("Chunk manager received shutdown signal");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Missed some messages but channel is still open, continue
                            tracing::debug!(missed = n, "Chunk manager broadcast receiver lagged");
                        }
                    }
                }

                // Periodic seal check
                _ = check_interval.tick() => {
                    if let Err(e) = self.run_seal_check().await {
                        tracing::error!(error = %e, "Seal check failed");
                    }

                    // Also run expiration check
                    if let Err(e) = self.run_expiration_check().await {
                        tracing::error!(error = %e, "Expiration check failed");
                    }
                }
            }
        }

        *self.status.write() = ServiceStatus::Stopped;
        tracing::debug!("Chunk manager stopped");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "chunk_manager"
    }

    fn status(&self) -> ServiceStatus {
        self.status.read().clone()
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure {
            max_retries: 5,
            backoff: Duration::from_secs(10),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Arc<LocalDiskEngine>) {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalDiskEngine::new(temp_dir.path().to_path_buf()).unwrap();
        (temp_dir, Arc::new(storage))
    }

    #[test]
    fn test_config_default() {
        let config = ChunkManagerConfig::default();
        assert_eq!(config.max_chunk_size, 1024 * 1024);
        assert_eq!(config.max_chunk_age, Duration::from_secs(3600));
        assert_eq!(config.max_points_per_chunk, 10_000);
        assert!(config.auto_rotation);
    }

    #[tokio::test]
    async fn test_chunk_manager_lifecycle() {
        let (_temp_dir, storage) = create_test_storage();
        let manager = ChunkManager::with_storage(storage);

        assert!(matches!(manager.status(), ServiceStatus::Stopped));

        // Start and stop via broadcast
        let (tx, rx) = broadcast::channel(1);

        let handle = tokio::spawn({
            let manager = Arc::new(manager);
            let m = manager.clone();
            async move { m.start(rx).await }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown
        tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stats_collection() {
        let (_temp_dir, storage) = create_test_storage();
        let manager = ChunkManager::with_storage(storage);

        let stats = manager.stats();
        assert_eq!(stats.chunks_sealed, 0);
        assert_eq!(stats.check_cycles, 0);
    }
}
