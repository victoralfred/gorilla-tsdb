//! Unified Cache Module for Kuba TSDB
//!
//! This module consolidates all cache-related functionality into a single,
//! maintainable location. It provides:
//!
//! - **Storage Cache**: Sharded LRU cache for chunk data (`storage.rs`)
//! - **Query Cache**: LRU cache for query results with TTL (`query.rs`)
//! - **Local Cache**: In-memory metadata cache for Redis lookups (`local.rs`)
//! - **Unified Stats**: Aggregate statistics across all cache layers (`unified.rs`)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                         Unified Cache Manager                                │
//! │                           (src/cache/unified.rs)                            │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                              │
//! │   ┌───────────────┐    ┌───────────────┐    ┌───────────────┐               │
//! │   │  Query Cache  │    │ Storage Cache │    │  Local Cache  │               │
//! │   │  128 MB, LRU  │    │ 256 MB, LRU   │    │ 1000 entries  │               │
//! │   │  60s TTL      │    │ 16 shards     │    │ 60s TTL       │               │
//! │   └───────────────┘    └───────────────┘    └───────────────┘               │
//! │                                                                              │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use kuba_tsdb::cache::{UnifiedCacheManager, UnifiedCacheStats};
//!
//! // Create unified manager
//! let manager = UnifiedCacheManager::new(query_cache, storage_cache);
//!
//! // Get aggregated stats
//! let stats = manager.stats();
//! println!("Overall hit rate: {:.2}%", stats.overall_hit_rate * 100.0);
//! ```

// Storage cache (sharded LRU for chunk data)
mod storage;
pub use storage::{
    CacheConfig as StorageCacheConfig, CacheEntry, CacheKey as StorageCacheKey, CacheManager,
    CacheShard, CacheStats as StorageCacheStats, EntryMetadata, LruList, MemoryTracker,
};

// Query cache (LRU with TTL for query results)
mod query;
pub use query::{
    CacheConfig as QueryCacheConfig, CacheKey as QueryCacheKey, CacheStats as QueryCacheStats,
    QueryCache, SharedQueryCache,
};

// Unified cache management
mod unified;
pub use unified::{
    QueryCacheStatsSnapshot, StorageCacheStatsSnapshot, UnifiedCacheManager, UnifiedCacheStats,
};

// Local metadata cache (for Redis index lookups)
mod local;
pub use local::{CachedMetadata, LocalMetadataCache};

// Cache invalidation via Redis Pub/Sub
mod invalidation;
pub use invalidation::{
    setup_cache_invalidation, InvalidationEvent, InvalidationPublisher, InvalidationSubscriber,
    PublisherStats, SubscriberConfig, SubscriberStats,
};
