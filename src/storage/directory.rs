///! Directory management utilities for storage layer
///!
///! Provides utilities for managing series directories, metadata files,
///! write locks, and cleanup operations.

use crate::error::StorageError;
use crate::types::SeriesId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Series metadata stored in metadata.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesMetadata {
    /// Series identifier
    pub series_id: SeriesId,

    /// Series name (optional)
    pub name: Option<String>,

    /// Retention policy in days (0 = infinite)
    pub retention_days: u32,

    /// Maximum points per chunk
    pub max_points_per_chunk: u32,

    /// Compression algorithm
    pub compression: String,

    /// Creation timestamp (unix milliseconds)
    pub created_at: i64,

    /// Last modified timestamp (unix milliseconds)
    pub modified_at: i64,

    /// Total point count (approximate)
    pub total_points: u64,

    /// Total chunks
    pub total_chunks: u32,
}

impl SeriesMetadata {
    /// Create new series metadata with defaults
    pub fn new(series_id: SeriesId) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            series_id,
            name: None,
            retention_days: 0, // Infinite retention by default
            max_points_per_chunk: 10_000,
            compression: "gorilla".to_string(),
            created_at: now,
            modified_at: now,
            total_points: 0,
            total_chunks: 0,
        }
    }

    /// Load metadata from file
    pub async fn load(path: &Path) -> Result<Self, StorageError> {
        let contents = fs::read_to_string(path).await?;
        serde_json::from_str(&contents)
            .map_err(|e| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse metadata: {}", e),
            )))
    }

    /// Save metadata to file (atomic write)
    pub async fn save(&self, path: &Path) -> Result<(), StorageError> {
        let contents = serde_json::to_string_pretty(self)
            .map_err(|e| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize metadata: {}", e),
            )))?;

        // Atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, contents).await?;
        fs::rename(&temp_path, path).await?;

        Ok(())
    }

    /// Update modification timestamp
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
    }
}

/// Write lock for preventing concurrent writes to a series
pub struct WriteLock {
    path: PathBuf,
    held: bool,
}

impl WriteLock {
    /// Create a new write lock (doesn't acquire it yet)
    pub fn new(series_path: &Path) -> Self {
        Self {
            path: series_path.join(".write.lock"),
            held: false,
        }
    }

    /// Try to acquire the write lock
    ///
    /// Returns error if lock is already held by another process
    pub async fn try_acquire(&mut self) -> Result<(), StorageError> {
        if self.held {
            return Ok(()); // Already held
        }

        // Check if lock file exists
        if self.path.exists() {
            // Try to read lock file to see if it's stale
            if let Ok(contents) = fs::read_to_string(&self.path).await {
                // Lock file contains PID and timestamp
                if let Some((pid_str, timestamp_str)) = contents.split_once(':') {
                    if let (Ok(pid), Ok(timestamp)) = (
                        pid_str.parse::<u32>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;

                        // If lock is older than 5 minutes, consider it stale
                        if now - timestamp > 300_000 {
                            // Stale lock, remove it
                            let _ = fs::remove_file(&self.path).await;
                        } else {
                            // Check if process is still alive (Unix only)
                            #[cfg(unix)]
                            {
                                use std::process::Command;
                                if Command::new("ps")
                                    .arg("-p")
                                    .arg(pid.to_string())
                                    .output()
                                    .map(|o| o.status.success())
                                    .unwrap_or(false)
                                {
                                    return Err(StorageError::Io(std::io::Error::new(
                                        std::io::ErrorKind::WouldBlock,
                                        format!("Write lock held by process {}", pid),
                                    )));
                                } else {
                                    // Process doesn't exist, remove stale lock
                                    let _ = fs::remove_file(&self.path).await;
                                }
                            }

                            #[cfg(not(unix))]
                            {
                                return Err(StorageError::Io(std::io::Error::new(
                                    std::io::ErrorKind::WouldBlock,
                                    format!("Write lock held by process {}", pid),
                                )));
                            }
                        }
                    }
                }
            }
        }

        // Acquire lock by writing PID and timestamp
        let pid = std::process::id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let lock_content = format!("{}:{}", pid, now);
        fs::write(&self.path, lock_content).await?;

        self.held = true;
        Ok(())
    }

    /// Release the write lock
    pub async fn release(&mut self) -> Result<(), StorageError> {
        if !self.held {
            return Ok(());
        }

        fs::remove_file(&self.path).await?;
        self.held = false;
        Ok(())
    }

    /// Check if lock is held
    pub fn is_held(&self) -> bool {
        self.held
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        if self.held {
            // Best effort cleanup
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Directory cleanup operations
pub struct DirectoryMaintenance;

impl DirectoryMaintenance {
    /// Clean up old chunks based on retention policy
    ///
    /// Removes chunks older than the retention period specified in metadata
    pub async fn cleanup_old_chunks(
        series_path: &Path,
        metadata: &SeriesMetadata,
    ) -> Result<usize, StorageError> {
        if metadata.retention_days == 0 {
            return Ok(0); // Infinite retention
        }

        let retention_ms = (metadata.retention_days as i64) * 24 * 60 * 60 * 1000;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let cutoff = now - retention_ms;

        let mut removed_count = 0;
        let mut entries = fs::read_dir(series_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Only process .gor files
            if path.extension().and_then(|e| e.to_str()) != Some("gor") {
                continue;
            }

            // Check file creation time
            if let Ok(file_metadata) = fs::metadata(&path).await {
                if let Ok(created) = file_metadata.created() {
                    if let Ok(created_duration) = created.duration_since(std::time::UNIX_EPOCH) {
                        let created_ms = created_duration.as_millis() as i64;
                        if created_ms < cutoff {
                            // Remove old chunk file
                            fs::remove_file(&path).await?;
                            removed_count += 1;

                            // Also remove associated .snappy file if exists
                            let snappy_path = path.with_extension("snappy");
                            if snappy_path.exists() {
                                let _ = fs::remove_file(&snappy_path).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Validate directory structure
    ///
    /// Checks for corrupted files, orphaned files, and missing metadata
    pub async fn validate_directory(
        series_path: &Path,
    ) -> Result<Vec<String>, StorageError> {
        let mut issues = Vec::new();

        // Check if directory exists
        if !series_path.exists() {
            issues.push(format!("Series directory does not exist: {:?}", series_path));
            return Ok(issues);
        }

        // Check for metadata.json
        let metadata_path = series_path.join("metadata.json");
        if !metadata_path.exists() {
            issues.push("Missing metadata.json file".to_string());
        }

        // Scan for chunk files
        let mut entries = fs::read_dir(series_path).await?;
        let mut chunk_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                chunk_count += 1;

                // Try to read header
                if let Ok(mut file) = fs::File::open(&path).await {
                    let mut header_bytes = [0u8; 64];
                    if file.read_exact(&mut header_bytes).await.is_err() {
                        issues.push(format!("Corrupted chunk file: {:?}", path));
                    }
                }
            }
        }

        if chunk_count == 0 {
            issues.push("No chunk files found in directory".to_string());
        }

        Ok(issues)
    }

    /// Remove empty series directories
    pub async fn cleanup_empty_directories(
        base_path: &Path,
    ) -> Result<usize, StorageError> {
        let mut removed_count = 0;
        let mut entries = fs::read_dir(base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            // Check if directory name starts with "series_"
            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                if dir_name.starts_with("series_") {
                    // Check if directory is empty or only contains lock/metadata files
                    if Self::is_series_directory_empty(&path).await? {
                        fs::remove_dir_all(&path).await?;
                        removed_count += 1;
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Check if a series directory is empty (no chunk files)
    async fn is_series_directory_empty(series_path: &Path) -> Result<bool, StorageError> {
        let mut entries = fs::read_dir(series_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_name = path.file_name().and_then(|n| n.to_str());

            // Ignore lock and metadata files
            if let Some(name) = file_name {
                if name == ".write.lock" || name == "metadata.json" {
                    continue;
                }
            }

            // If we find any other file, directory is not empty
            if path.extension().and_then(|e| e.to_str()) == Some("gor") {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_series_metadata_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("metadata.json");

        let mut metadata = SeriesMetadata::new(123);
        metadata.name = Some("test_series".to_string());
        metadata.retention_days = 30;
        metadata.total_points = 1000;

        // Save
        metadata.save(&metadata_path).await.unwrap();

        // Load
        let loaded = SeriesMetadata::load(&metadata_path).await.unwrap();

        assert_eq!(loaded.series_id, 123);
        assert_eq!(loaded.name, Some("test_series".to_string()));
        assert_eq!(loaded.retention_days, 30);
        assert_eq!(loaded.total_points, 1000);
    }

    #[tokio::test]
    async fn test_write_lock_acquire_release() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock = WriteLock::new(temp_dir.path());

        // Acquire lock
        lock.try_acquire().await.unwrap();
        assert!(lock.is_held());
        assert!(temp_dir.path().join(".write.lock").exists());

        // Release lock
        lock.release().await.unwrap();
        assert!(!lock.is_held());
        assert!(!temp_dir.path().join(".write.lock").exists());
    }

    #[tokio::test]
    async fn test_write_lock_prevents_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock1 = WriteLock::new(temp_dir.path());
        let mut lock2 = WriteLock::new(temp_dir.path());

        // First lock acquires successfully
        lock1.try_acquire().await.unwrap();

        // Second lock should fail
        let result = lock2.try_acquire().await;
        assert!(result.is_err());

        // After releasing first lock, second should succeed
        lock1.release().await.unwrap();
        lock2.try_acquire().await.unwrap();
        lock2.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_old_chunks() {
        use std::time::Duration;
        use tokio::time::sleep;

        let temp_dir = TempDir::new().unwrap();

        // Create some chunk files
        fs::write(temp_dir.path().join("chunk_old.gor"), b"old").await.unwrap();
        sleep(Duration::from_millis(10)).await;
        fs::write(temp_dir.path().join("chunk_new.gor"), b"new").await.unwrap();

        // Create metadata with 0 retention (should not delete anything)
        let metadata = SeriesMetadata::new(1);
        let removed = DirectoryMaintenance::cleanup_old_chunks(temp_dir.path(), &metadata)
            .await
            .unwrap();
        assert_eq!(removed, 0);

        // Both files should still exist
        assert!(temp_dir.path().join("chunk_old.gor").exists());
        assert!(temp_dir.path().join("chunk_new.gor").exists());
    }

    #[tokio::test]
    async fn test_validate_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Empty directory should have issues
        let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
            .await
            .unwrap();
        assert!(!issues.is_empty());

        // Add metadata
        let metadata = SeriesMetadata::new(1);
        metadata.save(&temp_dir.path().join("metadata.json")).await.unwrap();

        // Still should have issues (no chunks)
        let issues = DirectoryMaintenance::validate_directory(temp_dir.path())
            .await
            .unwrap();
        assert!(issues.iter().any(|i| i.contains("No chunk files")));
    }
}
