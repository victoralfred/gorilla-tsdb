//! Compression Metrics and Observability
//!
//! Provides detailed metrics about compression performance for monitoring
//! and optimization purposes.
//!
//! # Metrics Collected
//!
//! - Compression ratio per codec type
//! - Encoding/decoding latency histograms
//! - Bits per sample achieved
//! - Codec selection frequency
//! - Error rates
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::compression::metrics::{CompressionMetrics, CodecType};
//!
//! let metrics = CompressionMetrics::new();
//!
//! // Record a compression operation
//! metrics.record_compression(CodecType::Ahpac, 16000, 2000, 125);
//!
//! // Get current stats
//! let stats = metrics.snapshot();
//! println!("Overall compression ratio: {:.2}x", stats.overall_compression_ratio);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Compression codec types for metric tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecType {
    /// Kuba/Gorilla XOR compression
    Kuba,
    /// AHPAC adaptive compression
    Ahpac,
    /// ANS entropy coding
    Ans,
    /// Chimp compression
    Chimp,
    /// ALP (Adaptive Lossless Floating-Point)
    Alp,
    /// Delta + LZ4
    DeltaLz4,
    /// Snappy
    Snappy,
    /// No compression (raw)
    None,
}

impl CodecType {
    /// Get codec name as string
    pub fn name(&self) -> &'static str {
        match self {
            CodecType::Kuba => "kuba",
            CodecType::Ahpac => "ahpac",
            CodecType::Ans => "ans",
            CodecType::Chimp => "chimp",
            CodecType::Alp => "alp",
            CodecType::DeltaLz4 => "delta_lz4",
            CodecType::Snappy => "snappy",
            CodecType::None => "none",
        }
    }

    /// Get all codec types
    pub fn all() -> &'static [CodecType] {
        &[
            CodecType::Kuba,
            CodecType::Ahpac,
            CodecType::Ans,
            CodecType::Chimp,
            CodecType::Alp,
            CodecType::DeltaLz4,
            CodecType::Snappy,
            CodecType::None,
        ]
    }
}

/// Atomic counters for a single codec's metrics
#[derive(Debug, Default)]
pub struct CodecMetrics {
    /// Total compression operations
    pub compress_count: AtomicU64,
    /// Total decompression operations
    pub decompress_count: AtomicU64,
    /// Total bytes before compression
    pub original_bytes: AtomicU64,
    /// Total bytes after compression
    pub compressed_bytes: AtomicU64,
    /// Total compression time in microseconds
    pub compress_time_us: AtomicU64,
    /// Total decompression time in microseconds
    pub decompress_time_us: AtomicU64,
    /// Total points compressed
    pub points_compressed: AtomicU64,
    /// Total compression errors
    pub compress_errors: AtomicU64,
    /// Total decompression errors
    pub decompress_errors: AtomicU64,
}

impl CodecMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful compression operation
    pub fn record_compression(
        &self,
        original_size: u64,
        compressed_size: u64,
        duration_us: u64,
        point_count: u64,
    ) {
        self.compress_count.fetch_add(1, Ordering::Relaxed);
        self.original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);
        self.compress_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.points_compressed
            .fetch_add(point_count, Ordering::Relaxed);
    }

    /// Record a successful decompression operation
    pub fn record_decompression(&self, compressed_size: u64, original_size: u64, duration_us: u64) {
        self.decompress_count.fetch_add(1, Ordering::Relaxed);
        self.compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);
        self.original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.decompress_time_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a compression error
    pub fn record_compress_error(&self) {
        self.compress_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a decompression error
    pub fn record_decompress_error(&self) {
        self.decompress_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        let compressed = self.compressed_bytes.load(Ordering::Relaxed);
        let original = self.original_bytes.load(Ordering::Relaxed);
        if compressed == 0 {
            0.0
        } else {
            original as f64 / compressed as f64
        }
    }

    /// Get bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        let compressed = self.compressed_bytes.load(Ordering::Relaxed);
        let points = self.points_compressed.load(Ordering::Relaxed);
        if points == 0 {
            0.0
        } else {
            (compressed * 8) as f64 / points as f64
        }
    }

    /// Get average compression time in microseconds
    pub fn avg_compress_time_us(&self) -> f64 {
        let count = self.compress_count.load(Ordering::Relaxed);
        let time = self.compress_time_us.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            time as f64 / count as f64
        }
    }

    /// Get average decompression time in microseconds
    pub fn avg_decompress_time_us(&self) -> f64 {
        let count = self.decompress_count.load(Ordering::Relaxed);
        let time = self.decompress_time_us.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            time as f64 / count as f64
        }
    }

    /// Create a snapshot of current metrics
    pub fn snapshot(&self) -> CodecMetricsSnapshot {
        CodecMetricsSnapshot {
            compress_count: self.compress_count.load(Ordering::Relaxed),
            decompress_count: self.decompress_count.load(Ordering::Relaxed),
            original_bytes: self.original_bytes.load(Ordering::Relaxed),
            compressed_bytes: self.compressed_bytes.load(Ordering::Relaxed),
            compress_time_us: self.compress_time_us.load(Ordering::Relaxed),
            decompress_time_us: self.decompress_time_us.load(Ordering::Relaxed),
            points_compressed: self.points_compressed.load(Ordering::Relaxed),
            compress_errors: self.compress_errors.load(Ordering::Relaxed),
            decompress_errors: self.decompress_errors.load(Ordering::Relaxed),
        }
    }
}

/// Non-atomic snapshot of codec metrics for serialization
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CodecMetricsSnapshot {
    /// Total compression operations
    pub compress_count: u64,
    /// Total decompression operations
    pub decompress_count: u64,
    /// Total bytes before compression
    pub original_bytes: u64,
    /// Total bytes after compression
    pub compressed_bytes: u64,
    /// Total compression time in microseconds
    pub compress_time_us: u64,
    /// Total decompression time in microseconds
    pub decompress_time_us: u64,
    /// Total points compressed
    pub points_compressed: u64,
    /// Total compression errors
    pub compress_errors: u64,
    /// Total decompression errors
    pub decompress_errors: u64,
}

impl CodecMetricsSnapshot {
    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            0.0
        } else {
            self.original_bytes as f64 / self.compressed_bytes as f64
        }
    }

    /// Get bits per sample
    pub fn bits_per_sample(&self) -> f64 {
        if self.points_compressed == 0 {
            0.0
        } else {
            (self.compressed_bytes * 8) as f64 / self.points_compressed as f64
        }
    }

    /// Get average compression time in microseconds
    pub fn avg_compress_time_us(&self) -> f64 {
        if self.compress_count == 0 {
            0.0
        } else {
            self.compress_time_us as f64 / self.compress_count as f64
        }
    }

    /// Get average decompression time in microseconds
    pub fn avg_decompress_time_us(&self) -> f64 {
        if self.decompress_count == 0 {
            0.0
        } else {
            self.decompress_time_us as f64 / self.decompress_count as f64
        }
    }
}

/// Global compression metrics collector
pub struct CompressionMetrics {
    /// Metrics per codec type
    pub kuba: CodecMetrics,
    /// AHPAC metrics
    pub ahpac: CodecMetrics,
    /// ANS metrics
    pub ans: CodecMetrics,
    /// Chimp metrics
    pub chimp: CodecMetrics,
    /// ALP metrics
    pub alp: CodecMetrics,
    /// Delta+LZ4 metrics
    pub delta_lz4: CodecMetrics,
    /// Snappy metrics
    pub snappy: CodecMetrics,
    /// No compression metrics
    pub none: CodecMetrics,
    /// Creation timestamp
    created_at: Instant,
}

impl CompressionMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            kuba: CodecMetrics::new(),
            ahpac: CodecMetrics::new(),
            ans: CodecMetrics::new(),
            chimp: CodecMetrics::new(),
            alp: CodecMetrics::new(),
            delta_lz4: CodecMetrics::new(),
            snappy: CodecMetrics::new(),
            none: CodecMetrics::new(),
            created_at: Instant::now(),
        }
    }

    /// Get metrics for a specific codec type
    pub fn get(&self, codec: CodecType) -> &CodecMetrics {
        match codec {
            CodecType::Kuba => &self.kuba,
            CodecType::Ahpac => &self.ahpac,
            CodecType::Ans => &self.ans,
            CodecType::Chimp => &self.chimp,
            CodecType::Alp => &self.alp,
            CodecType::DeltaLz4 => &self.delta_lz4,
            CodecType::Snappy => &self.snappy,
            CodecType::None => &self.none,
        }
    }

    /// Record a compression operation
    pub fn record_compression(
        &self,
        codec: CodecType,
        original_size: u64,
        compressed_size: u64,
        duration_us: u64,
        point_count: u64,
    ) {
        self.get(codec).record_compression(
            original_size,
            compressed_size,
            duration_us,
            point_count,
        );
    }

    /// Record a decompression operation
    pub fn record_decompression(
        &self,
        codec: CodecType,
        compressed_size: u64,
        original_size: u64,
        duration_us: u64,
    ) {
        self.get(codec)
            .record_decompression(compressed_size, original_size, duration_us);
    }

    /// Record a compression error
    pub fn record_compress_error(&self, codec: CodecType) {
        self.get(codec).record_compress_error();
    }

    /// Record a decompression error
    pub fn record_decompress_error(&self, codec: CodecType) {
        self.get(codec).record_decompress_error();
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Create a full snapshot of all metrics
    pub fn snapshot(&self) -> CompressionMetricsSnapshot {
        let mut per_codec = std::collections::HashMap::new();
        for codec in CodecType::all() {
            per_codec.insert(codec.name().to_string(), self.get(*codec).snapshot());
        }

        // Calculate totals
        let mut total = CodecMetricsSnapshot::default();
        for snapshot in per_codec.values() {
            total.compress_count += snapshot.compress_count;
            total.decompress_count += snapshot.decompress_count;
            total.original_bytes += snapshot.original_bytes;
            total.compressed_bytes += snapshot.compressed_bytes;
            total.compress_time_us += snapshot.compress_time_us;
            total.decompress_time_us += snapshot.decompress_time_us;
            total.points_compressed += snapshot.points_compressed;
            total.compress_errors += snapshot.compress_errors;
            total.decompress_errors += snapshot.decompress_errors;
        }

        let overall_compression_ratio = total.compression_ratio();
        let overall_bits_per_sample = total.bits_per_sample();

        CompressionMetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            per_codec,
            total,
            overall_compression_ratio,
            overall_bits_per_sample,
        }
    }

    /// Format as Prometheus metrics
    pub fn to_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = String::new();

        // Total compression ratio
        output.push_str("# HELP compression_ratio Overall compression ratio\n");
        output.push_str("# TYPE compression_ratio gauge\n");
        output.push_str(&format!(
            "compression_ratio {:.4}\n",
            snapshot.overall_compression_ratio
        ));

        // Bits per sample
        output.push_str("# HELP compression_bits_per_sample Average bits per sample\n");
        output.push_str("# TYPE compression_bits_per_sample gauge\n");
        output.push_str(&format!(
            "compression_bits_per_sample {:.4}\n",
            snapshot.overall_bits_per_sample
        ));

        // Per-codec metrics
        output.push_str(
            "# HELP compression_operations_total Total compression operations by codec\n",
        );
        output.push_str("# TYPE compression_operations_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.compress_count > 0 {
                output.push_str(&format!(
                    "compression_operations_total{{codec=\"{}\",operation=\"compress\"}} {}\n",
                    codec, metrics.compress_count
                ));
            }
            if metrics.decompress_count > 0 {
                output.push_str(&format!(
                    "compression_operations_total{{codec=\"{}\",operation=\"decompress\"}} {}\n",
                    codec, metrics.decompress_count
                ));
            }
        }

        // Bytes processed
        output.push_str("# HELP compression_bytes_total Total bytes processed by codec\n");
        output.push_str("# TYPE compression_bytes_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.original_bytes > 0 {
                output.push_str(&format!(
                    "compression_bytes_total{{codec=\"{}\",type=\"original\"}} {}\n",
                    codec, metrics.original_bytes
                ));
                output.push_str(&format!(
                    "compression_bytes_total{{codec=\"{}\",type=\"compressed\"}} {}\n",
                    codec, metrics.compressed_bytes
                ));
            }
        }

        // Errors
        output.push_str("# HELP compression_errors_total Total compression errors by codec\n");
        output.push_str("# TYPE compression_errors_total counter\n");
        for (codec, metrics) in &snapshot.per_codec {
            if metrics.compress_errors > 0 || metrics.decompress_errors > 0 {
                output.push_str(&format!(
                    "compression_errors_total{{codec=\"{}\",operation=\"compress\"}} {}\n",
                    codec, metrics.compress_errors
                ));
                output.push_str(&format!(
                    "compression_errors_total{{codec=\"{}\",operation=\"decompress\"}} {}\n",
                    codec, metrics.decompress_errors
                ));
            }
        }

        output
    }
}

impl Default for CompressionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Full snapshot of compression metrics for serialization
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompressionMetricsSnapshot {
    /// Uptime in seconds
    pub uptime_secs: u64,
    /// Per-codec metrics
    pub per_codec: std::collections::HashMap<String, CodecMetricsSnapshot>,
    /// Total metrics across all codecs
    pub total: CodecMetricsSnapshot,
    /// Overall compression ratio
    pub overall_compression_ratio: f64,
    /// Overall bits per sample
    pub overall_bits_per_sample: f64,
}

/// RAII guard for timing compression operations
pub struct CompressionTimer {
    start: Instant,
    codec: CodecType,
    original_size: u64,
    point_count: u64,
    metrics: Arc<CompressionMetrics>,
    completed: bool,
}

impl CompressionTimer {
    /// Create a new timer for a compression operation
    pub fn new(
        metrics: Arc<CompressionMetrics>,
        codec: CodecType,
        original_size: u64,
        point_count: u64,
    ) -> Self {
        Self {
            start: Instant::now(),
            codec,
            original_size,
            point_count,
            metrics,
            completed: false,
        }
    }

    /// Complete the timer with the compressed size
    pub fn complete(mut self, compressed_size: u64) {
        let duration_us = self.start.elapsed().as_micros() as u64;
        self.metrics.record_compression(
            self.codec,
            self.original_size,
            compressed_size,
            duration_us,
            self.point_count,
        );
        self.completed = true;
    }

    /// Mark as failed
    pub fn fail(mut self) {
        self.metrics.record_compress_error(self.codec);
        self.completed = true;
    }
}

impl Drop for CompressionTimer {
    fn drop(&mut self) {
        if !self.completed {
            // If not completed, assume failure
            self.metrics.record_compress_error(self.codec);
        }
    }
}

/// Global metrics instance (lazy initialization)
static GLOBAL_METRICS: std::sync::OnceLock<Arc<CompressionMetrics>> = std::sync::OnceLock::new();

/// Get the global compression metrics instance
pub fn global_metrics() -> Arc<CompressionMetrics> {
    GLOBAL_METRICS
        .get_or_init(|| Arc::new(CompressionMetrics::new()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_metrics_basic() {
        let metrics = CodecMetrics::new();

        metrics.record_compression(16000, 2000, 100, 1000);
        metrics.record_compression(16000, 2000, 100, 1000);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.compress_count, 2);
        assert_eq!(snapshot.original_bytes, 32000);
        assert_eq!(snapshot.compressed_bytes, 4000);
        assert_eq!(snapshot.points_compressed, 2000);
        assert!((snapshot.compression_ratio() - 8.0).abs() < 0.01);
    }

    #[test]
    fn test_compression_metrics_per_codec() {
        let metrics = CompressionMetrics::new();

        metrics.record_compression(CodecType::Ahpac, 16000, 2000, 100, 1000);
        metrics.record_compression(CodecType::Kuba, 16000, 4000, 50, 1000);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.per_codec["ahpac"].compress_count, 1);
        assert_eq!(snapshot.per_codec["kuba"].compress_count, 1);
        assert_eq!(snapshot.total.compress_count, 2);
    }

    #[test]
    fn test_bits_per_sample() {
        let metrics = CodecMetrics::new();

        // 1000 points, 2000 bytes compressed = 16 bits per sample
        metrics.record_compression(16000, 2000, 100, 1000);

        assert!((metrics.bits_per_sample() - 16.0).abs() < 0.01);
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = CompressionMetrics::new();
        metrics.record_compression(CodecType::Ahpac, 16000, 2000, 100, 1000);

        let prometheus = metrics.to_prometheus();
        assert!(prometheus.contains("compression_ratio"));
        assert!(prometheus.contains("ahpac"));
    }

    #[test]
    fn test_timer_complete() {
        let metrics = Arc::new(CompressionMetrics::new());
        let timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);

        timer.complete(2000);

        assert_eq!(metrics.ahpac.compress_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_timer_fail() {
        let metrics = Arc::new(CompressionMetrics::new());
        let timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);

        timer.fail();

        assert_eq!(metrics.ahpac.compress_count.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_timer_drop_records_error() {
        let metrics = Arc::new(CompressionMetrics::new());
        {
            let _timer = CompressionTimer::new(metrics.clone(), CodecType::Ahpac, 16000, 1000);
            // Timer dropped without calling complete() or fail()
        }

        assert_eq!(metrics.ahpac.compress_errors.load(Ordering::Relaxed), 1);
    }
}
