//! Gorilla compression implementation
//!
//! Based on Facebook's Gorilla compression algorithm:
//! - Delta-of-delta timestamp compression
//! - XOR floating-point value compression
//!
//! Reference: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use super::bit_stream::{BitReader, BitWriter};
use crate::engine::traits::{
    BlockMetadata, CompressedBlock, Compressor, CompressionStats,
};
use crate::error::CompressionError;
use crate::types::DataPoint;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::mem::size_of_val;
use std::sync::Arc;

/// Gorilla compression implementation
pub struct GorillaCompressor {
    stats: Arc<Mutex<CompressionStats>>,
    config: GorillaConfig,
}

/// Configuration for Gorilla compressor
#[derive(Clone, Debug)]
pub struct GorillaConfig {
    /// Target block size (number of points)
    pub block_size: usize,
    /// Enable checksum verification
    pub enable_checksum: bool,
}

impl Default for GorillaConfig {
    fn default() -> Self {
        Self {
            block_size: 1000,
            enable_checksum: true,
        }
    }
}

impl GorillaCompressor {
    /// Create a new Gorilla compressor with default configuration
    pub fn new() -> Self {
        Self::with_config(GorillaConfig::default())
    }

    /// Create a new Gorilla compressor with custom configuration
    pub fn with_config(config: GorillaConfig) -> Self {
        Self {
            stats: Arc::new(Mutex::new(CompressionStats::default())),
            config,
        }
    }

    /// Compress timestamps using delta-of-delta encoding
    fn compress_timestamps(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // Write first timestamp (64 bits)
        writer.write_bits(points[0].timestamp as u64, 64);

        if points.len() == 1 {
            return;
        }

        // Write second timestamp as delta (14 bits is usually enough for regular intervals)
        let first_delta = points[1].timestamp - points[0].timestamp;
        writer.write_bits(first_delta as u64, 64);

        // Delta-of-delta encoding for remaining timestamps
        let mut prev_timestamp = points[1].timestamp;
        let mut prev_delta = first_delta;

        for point in &points[2..] {
            let delta = point.timestamp - prev_timestamp;
            let dod = delta - prev_delta;

            // Encode delta-of-delta with variable bit packing
            if dod == 0 {
                // No change in delta: 1 bit
                writer.write_bit(false);
            } else if (-63..=64).contains(&dod) {
                // Small change: '10' + 7 bits
                writer.write_bits(0b10, 2);
                writer.write_bits(((dod + 63) as u64) & 0x7F, 7);
            } else if (-255..=256).contains(&dod) {
                // Medium change: '110' + 9 bits
                writer.write_bits(0b110, 3);
                writer.write_bits(((dod + 255) as u64) & 0x1FF, 9);
            } else if (-2047..=2048).contains(&dod) {
                // Large change: '1110' + 12 bits
                writer.write_bits(0b1110, 4);
                writer.write_bits(((dod + 2047) as u64) & 0xFFF, 12);
            } else {
                // Very large change: '1111' + 32 bits
                writer.write_bits(0b1111, 4);
                writer.write_bits(delta as u64, 32);
            }

            prev_timestamp = point.timestamp;
            prev_delta = delta;
        }
    }

    /// Decompress timestamps using delta-of-delta decoding
    fn decompress_timestamps(
        count: usize,
        reader: &mut BitReader,
    ) -> Result<Vec<i64>, CompressionError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut timestamps = Vec::with_capacity(count);

        // Read first timestamp
        let first_timestamp = reader.read_bits(64)? as i64;
        timestamps.push(first_timestamp);

        if count == 1 {
            return Ok(timestamps);
        }

        // Read second timestamp as delta
        let first_delta = reader.read_bits(64)? as i64;
        let second_timestamp = first_timestamp + first_delta;
        timestamps.push(second_timestamp);

        // Decode delta-of-delta for remaining timestamps
        let mut prev_timestamp = second_timestamp;
        let mut prev_delta = first_delta;

        for _ in 2..count {
            let dod = if !reader.read_bit()? {
                // Delta didn't change
                0
            } else {
                // Check control bits
                if !reader.read_bit()? {
                    // '10' - 7 bits
                    (reader.read_bits(7)? as i64) - 63
                } else if !reader.read_bit()? {
                    // '110' - 9 bits
                    (reader.read_bits(9)? as i64) - 255
                } else if !reader.read_bit()? {
                    // '1110' - 12 bits
                    (reader.read_bits(12)? as i64) - 2047
                } else {
                    // '1111' - 32 bits (full delta)
                    prev_delta = reader.read_bits(32)? as i64;
                    0 // dod = 0 since we read full delta
                }
            };

            let delta = prev_delta + dod;
            let timestamp = prev_timestamp + delta;
            timestamps.push(timestamp);

            prev_timestamp = timestamp;
            prev_delta = delta;
        }

        Ok(timestamps)
    }

    /// Compress values using XOR encoding
    fn compress_values(points: &[DataPoint], writer: &mut BitWriter) {
        if points.is_empty() {
            return;
        }

        // Write first value (64 bits)
        writer.write_bits(points[0].value.to_bits(), 64);

        let mut prev_value = points[0].value.to_bits();
        let mut prev_leading = 0u32;
        let mut prev_trailing = 0u32;

        for point in &points[1..] {
            let curr_value = point.value.to_bits();
            let xor = prev_value ^ curr_value;

            if xor == 0 {
                // Value didn't change: 1 bit
                writer.write_bit(false);
            } else {
                writer.write_bit(true);

                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();

                // Check if we can use the previous leading/trailing zeros
                if leading >= prev_leading && trailing >= prev_trailing {
                    // Use previous block info: '0' + meaningful bits
                    writer.write_bit(false);
                    let meaningful_bits = 64 - prev_leading - prev_trailing;
                    if meaningful_bits > 0 {
                        writer.write_bits(
                            xor >> prev_trailing,
                            meaningful_bits as u8,
                        );
                    }
                } else {
                    // New block info: '1' + 5-bit leading + 6-bit length + meaningful bits
                    writer.write_bit(true);
                    writer.write_bits(leading as u64, 5);

                    let meaningful_bits = 64 - leading - trailing;
                    writer.write_bits(meaningful_bits as u64, 6);

                    if meaningful_bits > 0 {
                        writer.write_bits(xor >> trailing, meaningful_bits as u8);
                    }

                    prev_leading = leading;
                    prev_trailing = trailing;
                }
            }

            prev_value = curr_value;
        }
    }

    /// Decompress values using XOR decoding
    fn decompress_values(
        count: usize,
        reader: &mut BitReader,
    ) -> Result<Vec<f64>, CompressionError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut values = Vec::with_capacity(count);

        // Read first value
        let first_value = f64::from_bits(reader.read_bits(64)?);
        values.push(first_value);

        let mut prev_value = first_value.to_bits();
        let mut prev_leading = 0u32;
        let mut prev_trailing = 0u32;

        for _ in 1..count {
            if !reader.read_bit()? {
                // Value didn't change
                values.push(f64::from_bits(prev_value));
                continue;
            }

            let xor = if !reader.read_bit()? {
                // Use previous block info
                let meaningful_bits = 64 - prev_leading - prev_trailing;
                if meaningful_bits > 0 {
                    reader.read_bits(meaningful_bits as u8)? << prev_trailing
                } else {
                    0
                }
            } else {
                // Read new block info
                let leading = reader.read_bits(5)? as u32;
                let meaningful_bits = reader.read_bits(6)? as u32;

                if meaningful_bits > 0 {
                    let bits = reader.read_bits(meaningful_bits as u8)?;
                    let trailing = 64 - leading - meaningful_bits;
                    prev_leading = leading;
                    prev_trailing = trailing;
                    bits << trailing
                } else {
                    prev_leading = leading;
                    prev_trailing = 64 - leading;
                    0
                }
            };

            let value_bits = prev_value ^ xor;
            values.push(f64::from_bits(value_bits));
            prev_value = value_bits;
        }

        Ok(values)
    }

    /// Calculate checksum for data
    fn calculate_checksum(data: &[u8]) -> u64 {
        crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182).checksum(data)
    }
}

impl Default for GorillaCompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Compressor for GorillaCompressor {
    fn algorithm_id(&self) -> &str {
        "gorilla-v1"
    }

    async fn compress(&self, points: &[DataPoint]) -> Result<CompressedBlock, CompressionError> {
        if points.is_empty() {
            return Err(CompressionError::InvalidData(
                "Cannot compress empty point set".to_string(),
            ));
        }

        let start = std::time::Instant::now();
        let mut writer = BitWriter::new();

        // Write point count
        writer.write_bits(points.len() as u64, 32);

        // Compress timestamps and values
        Self::compress_timestamps(points, &mut writer);
        Self::compress_values(points, &mut writer);

        let compressed_data = writer.finish();
        let compressed_size = compressed_data.len();
        let original_size = size_of_val(points);

        // Calculate checksum
        let checksum = if self.config.enable_checksum {
            Self::calculate_checksum(&compressed_data)
        } else {
            0
        };

        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_compressed += 1;
            stats.compression_time_ms += start.elapsed().as_millis() as u64;
            let ratio = original_size as f64 / compressed_size.max(1) as f64;
            stats.average_ratio = if stats.total_compressed == 1 {
                ratio
            } else {
                (stats.average_ratio * (stats.total_compressed - 1) as f64 + ratio)
                    / stats.total_compressed as f64
            };
        }

        Ok(CompressedBlock {
            algorithm_id: self.algorithm_id().to_string(),
            original_size,
            compressed_size,
            checksum,
            data: Bytes::from(compressed_data),
            metadata: BlockMetadata {
                start_timestamp: points.first().unwrap().timestamp,
                end_timestamp: points.last().unwrap().timestamp,
                point_count: points.len(),
                series_id: points[0].series_id,
            },
        })
    }

    async fn decompress(&self, block: &CompressedBlock) -> Result<Vec<DataPoint>, CompressionError> {
        let start = std::time::Instant::now();

        // Verify checksum if enabled
        if self.config.enable_checksum && block.checksum != 0 {
            let calculated = Self::calculate_checksum(&block.data);
            if calculated != block.checksum {
                return Err(CompressionError::CorruptedData(format!(
                    "Checksum mismatch: expected {}, got {}",
                    block.checksum, calculated
                )));
            }
        }

        let mut reader = BitReader::new(&block.data);

        // Read point count
        let count = reader.read_bits(32)? as usize;

        if count == 0 {
            return Ok(Vec::new());
        }

        // Decompress timestamps and values
        let timestamps = Self::decompress_timestamps(count, &mut reader)?;
        let values = Self::decompress_values(count, &mut reader)?;

        if timestamps.len() != values.len() {
            return Err(CompressionError::CorruptedData(
                "Timestamp and value count mismatch".to_string(),
            ));
        }

        // Reconstruct data points
        let points: Vec<DataPoint> = timestamps
            .into_iter()
            .zip(values)
            .map(|(timestamp, value)| DataPoint {
                series_id: block.metadata.series_id,
                timestamp,
                value,
            })
            .collect();

        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_decompressed += 1;
            stats.decompression_time_ms += start.elapsed().as_millis() as u64;
        }

        Ok(points)
    }

    fn estimate_ratio(&self, _sample: &[DataPoint]) -> f64 {
        // Typical Gorilla compression ratio based on research
        12.0
    }

    fn stats(&self) -> CompressionStats {
        self.stats.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_points(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| DataPoint {
                series_id: 1,
                timestamp: 1000 + (i as i64 * 10), // Regular 10ms intervals
                value: 100.0 + (i as f64 * 0.5),   // Gradually increasing values
            })
            .collect()
    }

    #[tokio::test]
    async fn test_compress_decompress_single_point() {
        let compressor = GorillaCompressor::new();
        let points = vec![DataPoint {
            series_id: 1,
            timestamp: 1000,
            value: 42.5,
        }];

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].timestamp, 1000);
        assert_eq!(decompressed[0].value, 42.5);
    }

    #[tokio::test]
    async fn test_compress_decompress_multiple_points() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(100);

        let compressed = compressor.compress(&points).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(decompressed.len(), points.len());

        for (original, decompressed) in points.iter().zip(decompressed.iter()) {
            assert_eq!(decompressed.timestamp, original.timestamp);
            assert_eq!(decompressed.value, original.value);
        }
    }

    #[tokio::test]
    async fn test_compression_ratio() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(1000);

        let original_size = std::mem::size_of_val(points.as_slice());
        let compressed = compressor.compress(&points).await.unwrap();

        let ratio = original_size as f64 / compressed.compressed_size as f64;
        println!("Compression ratio: {:.2}:1", ratio);

        // Should achieve at least 2:1 compression
        assert!(ratio > 2.0);
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        let compressor = GorillaCompressor::new();
        let points = create_test_points(10);

        let mut compressed = compressor.compress(&points).await.unwrap();

        // Corrupt the data
        let mut data = compressed.data.to_vec();
        if !data.is_empty() {
            data[0] ^= 0xFF;
        }
        compressed.data = Bytes::from(data);

        // Should fail checksum verification
        let result = compressor.decompress(&compressed).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_points() {
        let compressor = GorillaCompressor::new();
        let points: Vec<DataPoint> = vec![];

        let result = compressor.compress(&points).await;
        assert!(result.is_err());
    }
}
