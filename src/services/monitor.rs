//! Monitor Service
//!
//! Collects and aggregates system metrics for observability:
//! - Storage metrics (disk usage, chunk counts, series counts)
//! - Performance metrics (query latency, write throughput)
//! - Resource metrics (memory usage, CPU utilization)
//! - Custom application metrics

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::{interval, Instant};

use crate::storage::LocalDiskEngine;

use super::framework::{RestartPolicy, Service, ServiceError, ServiceStatus};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the monitor service
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Interval for collecting metrics
    pub collection_interval: Duration,

    /// Enable detailed storage metrics
    pub storage_metrics: bool,

    /// Enable performance metrics
    pub performance_metrics: bool,

    /// Enable resource metrics (memory, CPU)
    pub resource_metrics: bool,

    /// Maximum number of metric samples to retain
    pub max_samples: usize,

    /// Enable metric aggregation
    pub aggregation_enabled: bool,

    /// Maximum number of unique custom metrics (prevents memory exhaustion)
    pub max_custom_metrics: usize,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(10),
            storage_metrics: true,
            performance_metrics: true,
            resource_metrics: true,
            max_samples: 1000,
            aggregation_enabled: true,
            max_custom_metrics: 10_000, // Limit unique metric names
        }
    }
}

// ============================================================================
// Metric Types
// ============================================================================

/// A single metric sample with timestamp
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// Metric name
    pub name: String,

    /// Metric value
    pub value: f64,

    /// Collection timestamp
    pub timestamp: Instant,

    /// Optional labels for the metric
    pub labels: HashMap<String, String>,
}

impl MetricSample {
    /// Create a new metric sample
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
            timestamp: Instant::now(),
            labels: HashMap::new(),
        }
    }

    /// Add a label to the metric
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

/// Aggregated metric statistics
#[derive(Debug, Clone, Default)]
pub struct MetricStats {
    /// Number of samples
    pub count: u64,

    /// Sum of all values
    pub sum: f64,

    /// Minimum value
    pub min: f64,

    /// Maximum value
    pub max: f64,

    /// Mean value
    pub mean: f64,

    /// Last recorded value
    pub last: f64,
}

impl MetricStats {
    /// Create new stats from a single value
    pub fn from_value(value: f64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
            mean: value,
            last: value,
        }
    }

    /// Update stats with a new value
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.mean = self.sum / self.count as f64;
        self.last = value;
    }
}

// ============================================================================
// Storage Metrics
// ============================================================================

/// Metrics related to storage
#[derive(Debug, Clone, Default)]
pub struct StorageMetrics {
    /// Total number of series
    pub series_count: u64,

    /// Total number of chunks
    pub chunk_count: u64,

    /// Active (unsealed) chunk count
    pub active_chunk_count: u64,

    /// Total bytes used on disk
    pub disk_bytes_used: u64,

    /// Total data points stored
    pub total_points: u64,

    /// Compression ratio (compressed / uncompressed)
    pub compression_ratio: f64,
}

// ============================================================================
// Performance Metrics
// ============================================================================

/// Metrics related to performance
#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    /// Write operations per second
    pub writes_per_second: f64,

    /// Read operations per second
    pub reads_per_second: f64,

    /// Average write latency (microseconds)
    pub avg_write_latency_us: f64,

    /// Average read latency (microseconds)
    pub avg_read_latency_us: f64,

    /// P99 write latency (microseconds)
    pub p99_write_latency_us: f64,

    /// P99 read latency (microseconds)
    pub p99_read_latency_us: f64,

    /// Bytes written per second
    pub write_throughput_bytes: f64,

    /// Bytes read per second
    pub read_throughput_bytes: f64,
}

// ============================================================================
// Resource Metrics
// ============================================================================

/// Metrics related to system resources
#[derive(Debug, Clone, Default)]
pub struct ResourceMetrics {
    /// Memory used by the process (bytes)
    pub memory_used_bytes: u64,

    /// Memory allocated for buffers (bytes)
    pub buffer_memory_bytes: u64,

    /// Number of active file handles
    pub open_files: u64,

    /// Number of active threads
    pub thread_count: u64,
}

// ============================================================================
// Combined Metrics Snapshot
// ============================================================================

/// A complete snapshot of all metrics
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    /// When this snapshot was taken
    pub timestamp: Option<Instant>,

    /// Storage metrics
    pub storage: StorageMetrics,

    /// Performance metrics
    pub performance: PerformanceMetrics,

    /// Resource metrics
    pub resources: ResourceMetrics,

    /// Custom metrics with their stats
    pub custom: HashMap<String, MetricStats>,
}

// ============================================================================
// Monitor Service
// ============================================================================

/// Background service for collecting and aggregating metrics
///
/// The MonitorService periodically collects metrics from the storage engine
/// and other system components, providing observability into database
/// performance and resource usage.
pub struct MonitorService {
    /// Configuration
    config: MonitorConfig,

    /// Storage engine reference (used for collecting storage metrics)
    #[allow(dead_code)]
    storage: Arc<LocalDiskEngine>,

    /// Current service status
    status: RwLock<ServiceStatus>,

    /// Latest metrics snapshot
    snapshot: RwLock<MetricsSnapshot>,

    /// Custom metric aggregations
    custom_metrics: RwLock<HashMap<String, MetricStats>>,

    /// Collection cycle counter
    collection_count: AtomicU64,
}

impl MonitorService {
    /// Create a new monitor service
    pub fn new(config: MonitorConfig, storage: Arc<LocalDiskEngine>) -> Self {
        Self {
            config,
            storage,
            status: RwLock::new(ServiceStatus::Stopped),
            snapshot: RwLock::new(MetricsSnapshot::default()),
            custom_metrics: RwLock::new(HashMap::new()),
            collection_count: AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn with_storage(storage: Arc<LocalDiskEngine>) -> Self {
        Self::new(MonitorConfig::default(), storage)
    }

    /// Get the latest metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot.read().clone()
    }

    /// Get storage metrics
    pub fn storage_metrics(&self) -> StorageMetrics {
        self.snapshot.read().storage.clone()
    }

    /// Get performance metrics
    pub fn performance_metrics(&self) -> PerformanceMetrics {
        self.snapshot.read().performance.clone()
    }

    /// Get resource metrics
    pub fn resource_metrics(&self) -> ResourceMetrics {
        self.snapshot.read().resources.clone()
    }

    /// Record a custom metric sample
    ///
    /// Returns `true` if the metric was recorded, `false` if rejected due to limits.
    /// New metrics are rejected when the limit is reached, but existing metrics
    /// can still be updated.
    pub fn record(&self, sample: MetricSample) -> bool {
        if !self.config.aggregation_enabled {
            return false;
        }

        let mut custom = self.custom_metrics.write();

        // Check if this is a new metric and we're at the limit
        if !custom.contains_key(&sample.name) && custom.len() >= self.config.max_custom_metrics {
            tracing::warn!(
                metric_name = %sample.name,
                current_count = custom.len(),
                max_count = self.config.max_custom_metrics,
                "Custom metric rejected: limit reached"
            );
            return false;
        }

        custom
            .entry(sample.name)
            .and_modify(|stats| stats.update(sample.value))
            .or_insert_with(|| MetricStats::from_value(sample.value));
        true
    }

    /// Record a simple metric value
    ///
    /// Returns `true` if recorded, `false` if rejected due to limits.
    pub fn record_value(&self, name: impl Into<String>, value: f64) -> bool {
        self.record(MetricSample::new(name, value))
    }

    /// Get stats for a custom metric
    pub fn get_metric_stats(&self, name: &str) -> Option<MetricStats> {
        self.custom_metrics.read().get(name).cloned()
    }

    /// Run a single collection cycle
    async fn collect_metrics(&self) -> Result<(), ServiceError> {
        let start = Instant::now();

        let mut snapshot = MetricsSnapshot {
            timestamp: Some(start),
            ..Default::default()
        };

        // Collect storage metrics if enabled
        if self.config.storage_metrics {
            snapshot.storage = self.collect_storage_metrics().await?;
        }

        // Collect performance metrics if enabled
        if self.config.performance_metrics {
            snapshot.performance = self.collect_performance_metrics().await?;
        }

        // Collect resource metrics if enabled
        if self.config.resource_metrics {
            snapshot.resources = self.collect_resource_metrics().await?;
        }

        // Copy custom metrics
        snapshot.custom = self.custom_metrics.read().clone();

        // Update snapshot
        *self.snapshot.write() = snapshot;

        // Increment collection counter
        self.collection_count.fetch_add(1, Ordering::Relaxed);

        tracing::trace!(
            duration_us = start.elapsed().as_micros(),
            "Metrics collection completed"
        );

        Ok(())
    }

    /// Collect storage-related metrics
    async fn collect_storage_metrics(&self) -> Result<StorageMetrics, ServiceError> {
        // Real implementation would query the storage engine for these values
        // For now, return placeholder metrics
        Ok(StorageMetrics {
            series_count: 0,
            chunk_count: 0,
            active_chunk_count: 0,
            disk_bytes_used: 0,
            total_points: 0,
            compression_ratio: 0.0,
        })
    }

    /// Collect performance-related metrics
    async fn collect_performance_metrics(&self) -> Result<PerformanceMetrics, ServiceError> {
        // Real implementation would track operation counters and latencies
        // For now, return placeholder metrics
        Ok(PerformanceMetrics::default())
    }

    /// Collect resource-related metrics
    async fn collect_resource_metrics(&self) -> Result<ResourceMetrics, ServiceError> {
        // Real implementation would query system stats
        // For now, return placeholder metrics
        Ok(ResourceMetrics::default())
    }

    /// Get the number of collection cycles completed
    pub fn collection_count(&self) -> u64 {
        self.collection_count.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Service for MonitorService {
    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
        *self.status.write() = ServiceStatus::Running;
        tracing::debug!(
            interval_secs = self.config.collection_interval.as_secs(),
            "Monitor service started"
        );

        let mut collect_interval = interval(self.config.collection_interval);

        loop {
            tokio::select! {
                // Shutdown signal received
                result = shutdown.recv() => {
                    match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!("Monitor service received shutdown signal");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Missed some messages but channel is still open, continue
                            tracing::debug!(missed = n, "Monitor service broadcast receiver lagged");
                        }
                    }
                }

                // Periodic metric collection
                _ = collect_interval.tick() => {
                    if let Err(e) = self.collect_metrics().await {
                        tracing::error!(error = %e, "Metric collection failed");
                    }
                }
            }
        }

        *self.status.write() = ServiceStatus::Stopped;
        tracing::debug!("Monitor service stopped");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "monitor"
    }

    fn status(&self) -> ServiceStatus {
        self.status.read().clone()
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure {
            max_retries: 5,
            backoff: Duration::from_secs(5),
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
        let config = MonitorConfig::default();
        assert_eq!(config.collection_interval, Duration::from_secs(10));
        assert!(config.storage_metrics);
        assert!(config.performance_metrics);
        assert!(config.resource_metrics);
        assert!(config.aggregation_enabled);
    }

    #[test]
    fn test_metric_sample() {
        let sample = MetricSample::new("test_metric", 42.0)
            .with_label("host", "localhost")
            .with_label("service", "test");

        assert_eq!(sample.name, "test_metric");
        assert_eq!(sample.value, 42.0);
        assert_eq!(sample.labels.get("host"), Some(&"localhost".to_string()));
        assert_eq!(sample.labels.get("service"), Some(&"test".to_string()));
    }

    #[test]
    fn test_metric_stats() {
        let mut stats = MetricStats::from_value(10.0);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 10.0);
        assert_eq!(stats.mean, 10.0);

        stats.update(20.0);
        assert_eq!(stats.count, 2);
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 20.0);
        assert_eq!(stats.mean, 15.0);
        assert_eq!(stats.last, 20.0);

        stats.update(5.0);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, 5.0);
        assert_eq!(stats.max, 20.0);
    }

    #[tokio::test]
    async fn test_monitor_service_lifecycle() {
        let (_temp_dir, storage) = create_test_storage();
        let service = Arc::new(MonitorService::with_storage(storage));

        let (tx, rx) = broadcast::channel(1);

        let s = service.clone();
        let handle = tokio::spawn(async move { s.start(rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_metrics() {
        let (_temp_dir, storage) = create_test_storage();
        let service = MonitorService::with_storage(storage);

        service.record_value("test_counter", 1.0);
        service.record_value("test_counter", 2.0);
        service.record_value("test_counter", 3.0);

        let stats = service.get_metric_stats("test_counter").unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.sum, 6.0);
        assert_eq!(stats.mean, 2.0);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 3.0);
        assert_eq!(stats.last, 3.0);
    }

    #[tokio::test]
    async fn test_snapshot() {
        let (_temp_dir, storage) = create_test_storage();
        let service = MonitorService::with_storage(storage);

        // Initially empty snapshot
        let snapshot = service.snapshot();
        assert!(snapshot.timestamp.is_none());

        // After collection, snapshot should have timestamp
        service.collect_metrics().await.unwrap();
        let snapshot = service.snapshot();
        assert!(snapshot.timestamp.is_some());
    }

    #[tokio::test]
    async fn test_custom_metrics_limit() {
        let (_temp_dir, storage) = create_test_storage();
        let config = MonitorConfig {
            max_custom_metrics: 3,
            ..Default::default()
        };
        let service = MonitorService::new(config, storage);

        // First 3 metrics should succeed
        assert!(service.record_value("metric_1", 1.0));
        assert!(service.record_value("metric_2", 2.0));
        assert!(service.record_value("metric_3", 3.0));

        // Fourth new metric should be rejected
        assert!(!service.record_value("metric_4", 4.0));

        // But updating existing metrics should still work
        assert!(service.record_value("metric_1", 10.0));

        let stats = service.get_metric_stats("metric_1").unwrap();
        assert_eq!(stats.count, 2);
        assert_eq!(stats.last, 10.0);

        // New metric still rejected
        assert!(!service.record_value("metric_5", 5.0));
        assert!(service.get_metric_stats("metric_5").is_none());
    }
}
