//! Chunk definitions and management
//!
//! Chunks are the fundamental storage unit, containing compressed time-series data
//! along with metadata for efficient retrieval and validation.
//!
//! # Chunk Lifecycle
//!
//! ```text
//! Active → Sealed → Compressed
//!   ↓         ↓          ↓
//! Memory   Disk       Disk+Snappy
//! (fast)   (mmap)     (space-optimized)
//! ```

use crate::types::{ChunkId, SeriesId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Magic number identifying Gorilla chunk format: "GORILLA" in hex
pub const CHUNK_MAGIC: u32 = 0x474F5249;  // "GORI" (first 4 bytes of "GORILLA")

/// Current chunk format version
pub const CHUNK_VERSION: u16 = 1;

/// Chunk header containing metadata
///
/// The header is stored at the beginning of each chunk file and contains
/// essential information for reading and validating the chunk.
///
/// # Binary Layout (64 bytes total)
///
/// ```text
/// Offset | Size | Field
/// -------|------|------------------
///   0    |  4   | magic
///   4    |  2   | version
///   6    |  16  | series_id
///  22    |  8   | start_timestamp
///  30    |  8   | end_timestamp
///  38    |  4   | point_count
///  42    |  4   | compressed_size
///  46    |  4   | uncompressed_size
///  50    |  8   | checksum
///  58    |  1   | compression_type
///  59    |  1   | flags
///  60    |  4   | reserved
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    /// Magic number for format identification (0x474F5249)
    pub magic: u32,

    /// Format version number (currently 1)
    pub version: u16,

    /// Series this chunk belongs to
    pub series_id: SeriesId,

    /// First timestamp in chunk (milliseconds)
    pub start_timestamp: i64,

    /// Last timestamp in chunk (milliseconds)
    pub end_timestamp: i64,

    /// Number of data points in chunk
    pub point_count: u32,

    /// Size of compressed data in bytes
    pub compressed_size: u32,

    /// Original uncompressed size in bytes
    pub uncompressed_size: u32,

    /// CRC64 checksum of data
    pub checksum: u64,

    /// Compression algorithm used
    pub compression_type: CompressionType,

    /// Additional chunk flags
    pub flags: ChunkFlags,
}

impl ChunkHeader {
    /// Create a new chunk header
    pub fn new(series_id: SeriesId) -> Self {
        Self {
            magic: CHUNK_MAGIC,
            version: CHUNK_VERSION,
            series_id,
            start_timestamp: 0,
            end_timestamp: 0,
            point_count: 0,
            compressed_size: 0,
            uncompressed_size: 0,
            checksum: 0,
            compression_type: CompressionType::Gorilla,
            flags: ChunkFlags::empty(),
        }
    }

    /// Validate header magic and version
    pub fn validate(&self) -> Result<(), String> {
        if self.magic != CHUNK_MAGIC {
            return Err(format!("Invalid magic number: 0x{:08X}", self.magic));
        }
        if self.version > CHUNK_VERSION {
            return Err(format!("Unsupported version: {}", self.version));
        }
        if self.point_count == 0 {
            return Err("Empty chunk".to_string());
        }
        if self.start_timestamp > self.end_timestamp {
            return Err("Invalid time range".to_string());
        }
        Ok(())
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size == 0 {
            return 0.0;
        }
        self.uncompressed_size as f64 / self.compressed_size as f64
    }

    /// Serialize header to bytes (64 bytes total)
    ///
    /// Binary layout:
    /// ```text
    /// [0-3]   magic (u32)
    /// [4-5]   version (u16)
    /// [6-21]  series_id (u128)
    /// [22-29] start_timestamp (i64)
    /// [30-37] end_timestamp (i64)
    /// [38-41] point_count (u32)
    /// [42-45] compressed_size (u32)
    /// [46-49] uncompressed_size (u32)
    /// [50-57] checksum (u64)
    /// [58]    compression_type (u8)
    /// [59]    flags (u8)
    /// [60-63] reserved (4 bytes, all zeros)
    /// ```
    pub fn to_bytes(&self) -> [u8; 64] {
        let mut bytes = [0u8; 64];

        // Magic number (4 bytes)
        bytes[0..4].copy_from_slice(&self.magic.to_le_bytes());

        // Version (2 bytes)
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());

        // Series ID (16 bytes)
        bytes[6..22].copy_from_slice(&self.series_id.to_le_bytes());

        // Timestamps (8 bytes each)
        bytes[22..30].copy_from_slice(&self.start_timestamp.to_le_bytes());
        bytes[30..38].copy_from_slice(&self.end_timestamp.to_le_bytes());

        // Counts and sizes (4 bytes each)
        bytes[38..42].copy_from_slice(&self.point_count.to_le_bytes());
        bytes[42..46].copy_from_slice(&self.compressed_size.to_le_bytes());
        bytes[46..50].copy_from_slice(&self.uncompressed_size.to_le_bytes());

        // Checksum (8 bytes)
        bytes[50..58].copy_from_slice(&self.checksum.to_le_bytes());

        // Compression type (1 byte)
        bytes[58] = self.compression_type as u8;

        // Flags (1 byte)
        bytes[59] = self.flags.0;

        // Reserved (4 bytes) - already zeroed

        bytes
    }

    /// Deserialize header from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - 64-byte array containing serialized header
    ///
    /// # Returns
    ///
    /// Parsed ChunkHeader or error if invalid
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 64 {
            return Err(format!("Invalid header size: {} bytes (expected 64)", bytes.len()));
        }

        // Parse magic number
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Parse version
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);

        // Parse series ID (16 bytes)
        let series_id = u128::from_le_bytes([
            bytes[6], bytes[7], bytes[8], bytes[9],
            bytes[10], bytes[11], bytes[12], bytes[13],
            bytes[14], bytes[15], bytes[16], bytes[17],
            bytes[18], bytes[19], bytes[20], bytes[21],
        ]);

        // Parse timestamps
        let start_timestamp = i64::from_le_bytes([
            bytes[22], bytes[23], bytes[24], bytes[25],
            bytes[26], bytes[27], bytes[28], bytes[29],
        ]);
        let end_timestamp = i64::from_le_bytes([
            bytes[30], bytes[31], bytes[32], bytes[33],
            bytes[34], bytes[35], bytes[36], bytes[37],
        ]);

        // Parse counts and sizes
        let point_count = u32::from_le_bytes([bytes[38], bytes[39], bytes[40], bytes[41]]);
        let compressed_size = u32::from_le_bytes([bytes[42], bytes[43], bytes[44], bytes[45]]);
        let uncompressed_size = u32::from_le_bytes([bytes[46], bytes[47], bytes[48], bytes[49]]);

        // Parse checksum
        let checksum = u64::from_le_bytes([
            bytes[50], bytes[51], bytes[52], bytes[53],
            bytes[54], bytes[55], bytes[56], bytes[57],
        ]);

        // Parse compression type
        let compression_type = match bytes[58] {
            0 => CompressionType::None,
            1 => CompressionType::Gorilla,
            2 => CompressionType::Snappy,
            3 => CompressionType::GorillaSnappy,
            n => return Err(format!("Invalid compression type: {}", n)),
        };

        // Parse flags
        let flags = ChunkFlags(bytes[59]);

        Ok(Self {
            magic,
            version,
            series_id,
            start_timestamp,
            end_timestamp,
            point_count,
            compressed_size,
            uncompressed_size,
            checksum,
            compression_type,
            flags,
        })
    }
}

/// Compression type for chunk data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression
    None = 0,
    /// Gorilla compression (default)
    Gorilla = 1,
    /// Snappy compression (for cold storage)
    Snappy = 2,
    /// Gorilla + Snappy (maximum compression)
    GorillaSnappy = 3,
}

/// Chunk flags for additional metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFlags(u8);

impl ChunkFlags {
    /// No flags set
    pub fn empty() -> Self {
        Self(0)
    }

    /// Chunk is sealed (immutable)
    pub fn sealed() -> Self {
        Self(0x01)
    }

    /// Chunk is compressed with Snappy
    pub fn snappy_compressed() -> Self {
        Self(0x02)
    }

    /// Check if sealed
    pub fn is_sealed(&self) -> bool {
        self.0 & 0x01 != 0
    }

    /// Check if Snappy compressed
    pub fn is_snappy_compressed(&self) -> bool {
        self.0 & 0x02 != 0
    }
}

/// Chunk metadata for tracking stored chunks
///
/// This is a lightweight structure used by the storage engine to track
/// chunks without loading the full data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Unique chunk identifier
    pub chunk_id: ChunkId,

    /// Series this chunk belongs to
    pub series_id: SeriesId,

    /// File path to chunk data
    pub path: PathBuf,

    /// Time range covered by this chunk
    /// Start timestamp in milliseconds
    pub start_timestamp: i64,
    /// End timestamp in milliseconds
    pub end_timestamp: i64,

    /// Number of points in chunk
    pub point_count: u32,

    /// Size on disk
    pub size_bytes: u64,

    /// Compression type used
    pub compression: CompressionType,

    /// When chunk was created
    pub created_at: i64,

    /// When chunk was last accessed
    pub last_accessed: i64,
}

impl ChunkMetadata {
    /// Check if chunk overlaps with time range
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        // Chunk overlaps if: chunk.start <= range.end AND chunk.end >= range.start
        self.start_timestamp <= end && self.end_timestamp >= start
    }

    /// Check if chunk fully contains time range
    pub fn contains(&self, start: i64, end: i64) -> bool {
        self.start_timestamp <= start && self.end_timestamp >= end
    }

    /// Get time span in milliseconds
    pub fn duration_ms(&self) -> i64 {
        self.end_timestamp - self.start_timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_header_validation() {
        let mut header = ChunkHeader::new(1);
        header.point_count = 100;
        header.start_timestamp = 1000;
        header.end_timestamp = 2000;

        assert!(header.validate().is_ok());

        // Invalid magic
        let mut bad = header.clone();
        bad.magic = 0x12345678;
        assert!(bad.validate().is_err());

        // Invalid time range
        let mut bad = header.clone();
        bad.start_timestamp = 2000;
        bad.end_timestamp = 1000;
        assert!(bad.validate().is_err());

        // Empty chunk
        let mut bad = header.clone();
        bad.point_count = 0;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_chunk_metadata_overlap() {
        let metadata = ChunkMetadata {
            chunk_id: ChunkId::new(),
            series_id: 1,
            path: PathBuf::from("/test"),
            start_timestamp: 1000,
            end_timestamp: 2000,
            point_count: 100,
            size_bytes: 1024,
            compression: CompressionType::Gorilla,
            created_at: 0,
            last_accessed: 0,
        };

        // Overlapping ranges
        assert!(metadata.overlaps(500, 1500));   // Partial overlap (start)
        assert!(metadata.overlaps(1500, 2500));  // Partial overlap (end)
        assert!(metadata.overlaps(1200, 1800));  // Fully contained
        assert!(metadata.overlaps(500, 2500));   // Fully contains chunk

        // Non-overlapping ranges
        assert!(!metadata.overlaps(0, 999));     // Before
        assert!(!metadata.overlaps(2001, 3000)); // After
    }

    #[test]
    fn test_chunk_flags() {
        let empty = ChunkFlags::empty();
        assert!(!empty.is_sealed());
        assert!(!empty.is_snappy_compressed());

        let sealed = ChunkFlags::sealed();
        assert!(sealed.is_sealed());
        assert!(!sealed.is_snappy_compressed());

        let compressed = ChunkFlags::snappy_compressed();
        assert!(!compressed.is_sealed());
        assert!(compressed.is_snappy_compressed());
    }

    #[test]
    fn test_chunk_header_serialization() {
        let mut header = ChunkHeader::new(12345);
        header.start_timestamp = 1000;
        header.end_timestamp = 2000;
        header.point_count = 100;
        header.compressed_size = 1024;
        header.uncompressed_size = 2048;
        header.checksum = 0x123456789ABCDEF0;
        header.compression_type = CompressionType::Gorilla;
        header.flags = ChunkFlags::sealed();

        // Serialize to bytes
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 64);

        // Verify magic number
        assert_eq!(&bytes[0..4], &CHUNK_MAGIC.to_le_bytes());

        // Deserialize back
        let decoded = ChunkHeader::from_bytes(&bytes).unwrap();

        // Verify all fields match
        assert_eq!(decoded.magic, header.magic);
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.series_id, header.series_id);
        assert_eq!(decoded.start_timestamp, header.start_timestamp);
        assert_eq!(decoded.end_timestamp, header.end_timestamp);
        assert_eq!(decoded.point_count, header.point_count);
        assert_eq!(decoded.compressed_size, header.compressed_size);
        assert_eq!(decoded.uncompressed_size, header.uncompressed_size);
        assert_eq!(decoded.checksum, header.checksum);
        assert_eq!(decoded.compression_type, header.compression_type);
        assert_eq!(decoded.flags.0, header.flags.0);
    }

    #[test]
    fn test_chunk_header_deserialization_invalid() {
        // Too short
        let short_bytes = vec![0u8; 32];
        assert!(ChunkHeader::from_bytes(&short_bytes).is_err());

        // Invalid compression type
        let mut bytes = [0u8; 64];
        bytes[0..4].copy_from_slice(&CHUNK_MAGIC.to_le_bytes());
        bytes[58] = 99; // Invalid compression type
        assert!(ChunkHeader::from_bytes(&bytes).is_err());
    }
}
