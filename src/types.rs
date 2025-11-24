//! Core data types used throughout the database

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for a time-series
pub type SeriesId = u128;

/// Unique identifier for a chunk
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub String);

impl ChunkId {
    /// Create a new chunk ID
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Create from a string
    pub fn from_string(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Default for ChunkId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A single data point in a time-series
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    /// Series identifier
    pub series_id: SeriesId,
    /// Unix timestamp in milliseconds
    pub timestamp: i64,
    /// Float value
    pub value: f64,
}

impl DataPoint {
    /// Create a new data point
    pub fn new(series_id: SeriesId, timestamp: i64, value: f64) -> Self {
        Self {
            series_id,
            timestamp,
            value,
        }
    }
}

/// Time range for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: i64,
    /// End timestamp (inclusive)
    pub end: i64,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp falls within this range
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    /// Get the duration of this range in milliseconds
    pub fn duration_ms(&self) -> i64 {
        self.end - self.start
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self {
            start: 0,
            end: i64::MAX,
        }
    }
}

/// Tag set for metrics
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSet {
    /// Tag key-value pairs
    pub tags: HashMap<String, String>,
}

impl TagSet {
    /// Create a new empty tag set
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    /// Create from a hashmap
    pub fn from_map(tags: HashMap<String, String>) -> Self {
        Self { tags }
    }

    /// Add a tag
    pub fn add(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Get a tag value
    pub fn get(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }

    /// Calculate a hash for this tag set
    pub fn hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Sort keys for consistent hashing
        let mut keys: Vec<_> = self.tags.keys().collect();
        keys.sort();

        for key in keys {
            key.hash(&mut hasher);
            self.tags[key].hash(&mut hasher);
        }

        hasher.finish()
    }
}

impl Default for TagSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Tag filter for queries
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagFilter {
    /// Match all tags
    All,
    /// Exact match on tags
    Exact(HashMap<String, String>),
    /// Pattern matching (not yet implemented)
    Pattern(String),
}

impl TagFilter {
    /// Check if a tag set matches this filter
    pub fn matches(&self, tags: &HashMap<String, String>) -> bool {
        match self {
            TagFilter::All => true,
            TagFilter::Exact(filter_tags) => {
                filter_tags.iter().all(|(k, v)| tags.get(k) == Some(v))
            }
            TagFilter::Pattern(_) => {
                // TODO: Implement pattern matching
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range() {
        let range = TimeRange::new(100, 200);
        assert!(range.contains(150));
        assert!(!range.contains(50));
        assert!(!range.contains(250));
        assert_eq!(range.duration_ms(), 100);
    }

    #[test]
    fn test_tag_set_hash() {
        let mut tags1 = TagSet::new();
        tags1.add("host".to_string(), "server1".to_string());
        tags1.add("dc".to_string(), "us-east".to_string());

        let mut tags2 = TagSet::new();
        tags2.add("dc".to_string(), "us-east".to_string());
        tags2.add("host".to_string(), "server1".to_string());

        // Hashes should be equal regardless of insertion order
        assert_eq!(tags1.hash(), tags2.hash());
    }

    #[test]
    fn test_tag_filter() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("dc".to_string(), "us-east".to_string());

        let filter = TagFilter::All;
        assert!(filter.matches(&tags));

        let mut filter_tags = HashMap::new();
        filter_tags.insert("host".to_string(), "server1".to_string());
        let filter = TagFilter::Exact(filter_tags);
        assert!(filter.matches(&tags));

        let mut filter_tags = HashMap::new();
        filter_tags.insert("host".to_string(), "server2".to_string());
        let filter = TagFilter::Exact(filter_tags);
        assert!(!filter.matches(&tags));
    }
}
