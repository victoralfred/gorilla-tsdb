//! Prometheus metrics exporter for ingestion pipeline
//!
//! Provides formatted Prometheus metrics output for monitoring systems.
//! Integrates with the global metrics registry and ingestion-specific metrics.

use std::fmt::Write;
use std::sync::Arc;

use prometheus::{Encoder, TextEncoder};

use crate::ingestion::metrics::{IngestionMetrics, MetricsSnapshot};

/// Configuration for Prometheus exporter
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// Prefix for all metric names (default: "gorilla_tsdb_ingestion")
    pub metric_prefix: String,
    /// Include process metrics (CPU, memory, etc.)
    pub include_process_metrics: bool,
    /// Include Go-style runtime metrics
    pub include_runtime_metrics: bool,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            metric_prefix: "gorilla_tsdb_ingestion".to_string(),
            include_process_metrics: true,
            include_runtime_metrics: false,
        }
    }
}

/// Prometheus metrics exporter
///
/// Formats metrics in Prometheus text exposition format for scraping
/// by Prometheus or compatible monitoring systems.
pub struct PrometheusExporter {
    /// Configuration
    config: PrometheusConfig,
    /// Reference to ingestion metrics
    ingestion_metrics: Arc<IngestionMetrics>,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter
    pub fn new(config: PrometheusConfig, ingestion_metrics: Arc<IngestionMetrics>) -> Self {
        Self {
            config,
            ingestion_metrics,
        }
    }

    /// Export metrics in Prometheus text format
    ///
    /// Returns a string containing all metrics in Prometheus exposition format.
    pub fn export(&self) -> String {
        let mut output = String::new();
        let snapshot = self.ingestion_metrics.snapshot();
        let prefix = &self.config.metric_prefix;

        // Add ingestion-specific metrics
        self.export_ingestion_metrics(&mut output, prefix, &snapshot);

        // Add global metrics from prometheus crate registry
        if let Ok(global_metrics) = self.export_global_metrics() {
            output.push_str(&global_metrics);
        }

        output
    }

    /// Export ingestion-specific metrics
    ///
    /// Uses `write!` macro directly to avoid intermediate string allocations
    /// from `format!` calls, improving performance for high-frequency metric exports.
    fn export_ingestion_metrics(
        &self,
        output: &mut String,
        prefix: &str,
        snapshot: &MetricsSnapshot,
    ) {
        // Points received counter
        let _ = writeln!(
            output,
            "# HELP {}_points_received_total Total data points received",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_points_received_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_points_received_total {}\n",
            prefix, snapshot.points_received
        );

        // Points written counter
        let _ = writeln!(
            output,
            "# HELP {}_points_written_total Total data points written to storage",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_points_written_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_points_written_total {}\n",
            prefix, snapshot.points_written
        );

        // Points rejected counter
        let _ = writeln!(
            output,
            "# HELP {}_points_rejected_total Total data points rejected due to backpressure",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_points_rejected_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_points_rejected_total {}\n",
            prefix, snapshot.points_rejected
        );

        // Points dropped counter
        let _ = writeln!(
            output,
            "# HELP {}_points_dropped_total Total data points dropped (validation failures)",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_points_dropped_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_points_dropped_total {}\n",
            prefix, snapshot.points_dropped
        );

        // Batches processed counter
        let _ = writeln!(
            output,
            "# HELP {}_batches_processed_total Total batches processed",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_batches_processed_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_batches_processed_total {}\n",
            prefix, snapshot.batches_processed
        );

        // Bytes written counter
        let _ = writeln!(
            output,
            "# HELP {}_bytes_written_total Total bytes written to storage",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_bytes_written_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_bytes_written_total {}\n",
            prefix, snapshot.bytes_written
        );

        // Write errors counter
        let _ = writeln!(
            output,
            "# HELP {}_write_errors_total Total write errors",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_write_errors_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_write_errors_total {}\n",
            prefix, snapshot.write_errors
        );

        // Write retries counter
        let _ = writeln!(
            output,
            "# HELP {}_write_retries_total Total write retries",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_write_retries_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_write_retries_total {}\n",
            prefix, snapshot.write_retries
        );

        // Backpressure activations counter
        let _ = writeln!(
            output,
            "# HELP {}_backpressure_activations_total Times backpressure was activated",
            prefix
        );
        let _ = writeln!(
            output,
            "# TYPE {}_backpressure_activations_total counter",
            prefix
        );
        let _ = writeln!(
            output,
            "{}_backpressure_activations_total {}\n",
            prefix, snapshot.backpressure_activations
        );

        // Overwrites counter
        let _ = writeln!(
            output,
            "# HELP {}_overwrites_total Total timestamp overwrites (duplicates)",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_overwrites_total counter", prefix);
        let _ = writeln!(
            output,
            "{}_overwrites_total {}\n",
            prefix, snapshot.overwrites
        );

        // Average write latency gauge
        let _ = writeln!(
            output,
            "# HELP {}_write_latency_avg_microseconds Average write latency in microseconds",
            prefix
        );
        let _ = writeln!(
            output,
            "# TYPE {}_write_latency_avg_microseconds gauge",
            prefix
        );
        let _ = writeln!(
            output,
            "{}_write_latency_avg_microseconds {}\n",
            prefix, snapshot.avg_write_latency_us
        );

        // Average batch age gauge
        let _ = writeln!(
            output,
            "# HELP {}_batch_age_avg_microseconds Average batch age when flushed in microseconds",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_batch_age_avg_microseconds gauge", prefix);
        let _ = writeln!(
            output,
            "{}_batch_age_avg_microseconds {}\n",
            prefix, snapshot.avg_batch_age_us
        );

        // Uptime gauge
        let _ = writeln!(
            output,
            "# HELP {}_uptime_seconds System uptime in seconds",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_uptime_seconds gauge", prefix);
        let _ = writeln!(
            output,
            "{}_uptime_seconds {}\n",
            prefix,
            snapshot.uptime.as_secs()
        );

        // Throughput gauges
        let _ = writeln!(
            output,
            "# HELP {}_points_per_second Current write throughput in points/second",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_points_per_second gauge", prefix);
        let _ = writeln!(
            output,
            "{}_points_per_second {:.2}\n",
            prefix, snapshot.points_per_second
        );

        let _ = writeln!(
            output,
            "# HELP {}_bytes_per_second Current write throughput in bytes/second",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_bytes_per_second gauge", prefix);
        let _ = writeln!(
            output,
            "{}_bytes_per_second {:.2}\n",
            prefix, snapshot.bytes_per_second
        );

        // Derived metrics
        let _ = writeln!(
            output,
            "# HELP {}_write_success_rate Write success rate (0.0-1.0)",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_write_success_rate gauge", prefix);
        let _ = writeln!(
            output,
            "{}_write_success_rate {:.4}\n",
            prefix,
            snapshot.write_success_rate()
        );

        let _ = writeln!(
            output,
            "# HELP {}_rejection_rate Rejection rate due to backpressure (0.0-1.0)",
            prefix
        );
        let _ = writeln!(output, "# TYPE {}_rejection_rate gauge", prefix);
        let _ = writeln!(
            output,
            "{}_rejection_rate {:.4}\n",
            prefix,
            snapshot.rejection_rate()
        );
    }

    /// Export global metrics from prometheus crate registry
    fn export_global_metrics(&self) -> Result<String, String> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];

        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| format!("Failed to encode metrics: {}", e))?;

        String::from_utf8(buffer).map_err(|e| format!("Metrics contain invalid UTF-8: {}", e))
    }

    /// Export metrics as HTTP response body with correct content type
    pub fn export_http(&self) -> (String, &'static str) {
        let body = self.export();
        let content_type = "text/plain; version=0.0.4; charset=utf-8";
        (body, content_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_metrics() -> Arc<IngestionMetrics> {
        let metrics = Arc::new(IngestionMetrics::new());
        metrics.record_received(1000);
        metrics.record_write(800, 4000, Duration::from_micros(500));
        metrics.record_rejected(100);
        metrics.record_dropped(100);
        metrics
    }

    #[test]
    fn test_prometheus_config_default() {
        let config = PrometheusConfig::default();
        assert_eq!(config.metric_prefix, "gorilla_tsdb_ingestion");
        assert!(config.include_process_metrics);
    }

    #[test]
    fn test_prometheus_exporter_creation() {
        let config = PrometheusConfig::default();
        let metrics = create_test_metrics();
        let exporter = PrometheusExporter::new(config, metrics);
        assert!(!exporter.config.metric_prefix.is_empty());
    }

    #[test]
    fn test_prometheus_export() {
        let config = PrometheusConfig::default();
        let metrics = create_test_metrics();
        let exporter = PrometheusExporter::new(config, metrics);

        let output = exporter.export();

        // Check for expected metric names
        assert!(output.contains("gorilla_tsdb_ingestion_points_received_total"));
        assert!(output.contains("gorilla_tsdb_ingestion_points_written_total"));
        assert!(output.contains("gorilla_tsdb_ingestion_points_rejected_total"));
        assert!(output.contains("gorilla_tsdb_ingestion_bytes_written_total"));
        assert!(output.contains("gorilla_tsdb_ingestion_write_success_rate"));

        // Check for HELP and TYPE comments
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("counter"));
        assert!(output.contains("gauge"));
    }

    #[test]
    fn test_prometheus_export_values() {
        let config = PrometheusConfig::default();
        let metrics = create_test_metrics();
        let exporter = PrometheusExporter::new(config, metrics);

        let output = exporter.export();

        // Check for specific values
        assert!(output.contains("gorilla_tsdb_ingestion_points_received_total 1000"));
        assert!(output.contains("gorilla_tsdb_ingestion_points_written_total 800"));
        assert!(output.contains("gorilla_tsdb_ingestion_bytes_written_total 4000"));
    }

    #[test]
    fn test_prometheus_export_http() {
        let config = PrometheusConfig::default();
        let metrics = create_test_metrics();
        let exporter = PrometheusExporter::new(config, metrics);

        let (body, content_type) = exporter.export_http();

        assert!(!body.is_empty());
        assert!(content_type.contains("text/plain"));
        assert!(content_type.contains("version=0.0.4"));
    }

    #[test]
    fn test_custom_metric_prefix() {
        let config = PrometheusConfig {
            metric_prefix: "custom_prefix".to_string(),
            ..Default::default()
        };
        let metrics = create_test_metrics();
        let exporter = PrometheusExporter::new(config, metrics);

        let output = exporter.export();
        assert!(output.contains("custom_prefix_points_received_total"));
        assert!(!output.contains("gorilla_tsdb_ingestion_points_received_total"));
    }
}
