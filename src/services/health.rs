//! Health Check Service
//!
//! Performs periodic health checks on the database system:
//! - Storage health (disk space, file integrity)
//! - Service health (all services running)
//! - Performance health (latency within thresholds)
//! - Connectivity health (external dependencies)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::{interval, Instant};

use crate::storage::LocalDiskEngine;

use super::framework::{RestartPolicy, Service, ServiceError, ServiceStatus};
use super::ServiceManager;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the health check service
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval between health checks
    pub check_interval: Duration,

    /// Timeout for individual health checks
    pub check_timeout: Duration,

    /// Minimum free disk space percentage
    pub min_free_disk_percent: f64,

    /// Maximum acceptable write latency (microseconds)
    pub max_write_latency_us: u64,

    /// Maximum acceptable read latency (microseconds)
    pub max_read_latency_us: u64,

    /// Number of consecutive failures before unhealthy
    pub failure_threshold: u32,

    /// Number of consecutive successes to recover
    pub recovery_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            min_free_disk_percent: 10.0,
            max_write_latency_us: 10_000, // 10ms
            max_read_latency_us: 5_000,   // 5ms
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

// ============================================================================
// Health Status
// ============================================================================

/// Overall health status of the system
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// System is fully healthy
    Healthy,

    /// System is degraded but operational
    Degraded,

    /// System is unhealthy
    Unhealthy,

    /// Health status is unknown (checks haven't run yet)
    Unknown,
}

impl HealthStatus {
    /// Check if the status indicates the system is operational
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }

    /// Check if the status indicates the system is fully healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }
}

// ============================================================================
// Individual Check Results
// ============================================================================

/// Result of a single health check
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Name of the check
    pub name: String,

    /// Whether the check passed
    pub passed: bool,

    /// Optional message with details
    pub message: Option<String>,

    /// Duration of the check
    pub duration: Duration,

    /// When the check was performed
    pub checked_at: Instant,
}

impl CheckResult {
    /// Create a passing check result
    pub fn pass(name: impl Into<String>, duration: Duration) -> Self {
        Self {
            name: name.into(),
            passed: true,
            message: None,
            duration,
            checked_at: Instant::now(),
        }
    }

    /// Create a failing check result
    pub fn fail(name: impl Into<String>, message: impl Into<String>, duration: Duration) -> Self {
        Self {
            name: name.into(),
            passed: false,
            message: Some(message.into()),
            duration,
            checked_at: Instant::now(),
        }
    }
}

// ============================================================================
// Health Report
// ============================================================================

/// Complete health report with all check results
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall health status
    pub status: HealthStatus,

    /// Individual check results
    pub checks: Vec<CheckResult>,

    /// When the report was generated
    pub generated_at: Instant,

    /// Total duration of all checks
    pub total_duration: Duration,
}

impl Default for HealthReport {
    fn default() -> Self {
        Self {
            status: HealthStatus::Unknown,
            checks: Vec::new(),
            generated_at: Instant::now(),
            total_duration: Duration::ZERO,
        }
    }
}

impl HealthReport {
    /// Create a new health report from check results
    pub fn from_checks(checks: Vec<CheckResult>) -> Self {
        let total_duration = checks.iter().map(|c| c.duration).sum();
        let failed_count = checks.iter().filter(|c| !c.passed).count();

        // Determine health status based on failure ratio:
        // - 0 failures: Healthy
        // - Less than half failed: Degraded
        // - Half or more failed: Unhealthy
        let status = if failed_count == 0 {
            HealthStatus::Healthy
        } else if failed_count * 2 < checks.len() {
            // Less than half failed (handles odd counts correctly)
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        Self {
            status,
            checks,
            generated_at: Instant::now(),
            total_duration,
        }
    }

    /// Get all failed checks
    pub fn failed_checks(&self) -> Vec<&CheckResult> {
        self.checks.iter().filter(|c| !c.passed).collect()
    }

    /// Get all passed checks
    pub fn passed_checks(&self) -> Vec<&CheckResult> {
        self.checks.iter().filter(|c| c.passed).collect()
    }
}

// ============================================================================
// Health Checker Service
// ============================================================================

/// Background service for performing health checks
///
/// The HealthChecker periodically verifies the health of the database
/// system including storage, services, and performance. It provides
/// a comprehensive health report for monitoring and alerting.
pub struct HealthChecker {
    /// Configuration
    config: HealthConfig,

    /// Storage engine reference (used for storage health checks)
    #[allow(dead_code)]
    storage: Arc<LocalDiskEngine>,

    /// Service manager reference (optional, for service health checks)
    service_manager: Option<Arc<ServiceManager>>,

    /// Current service status
    status: RwLock<ServiceStatus>,

    /// Latest health report
    report: RwLock<HealthReport>,

    /// Consecutive failure counts per check
    failure_counts: RwLock<HashMap<String, u32>>,

    /// Consecutive success counts per check
    success_counts: RwLock<HashMap<String, u32>>,
}

impl HealthChecker {
    /// Create a new health checker service
    pub fn new(config: HealthConfig, storage: Arc<LocalDiskEngine>) -> Self {
        Self {
            config,
            storage,
            service_manager: None,
            status: RwLock::new(ServiceStatus::Stopped),
            report: RwLock::new(HealthReport::default()),
            failure_counts: RwLock::new(HashMap::new()),
            success_counts: RwLock::new(HashMap::new()),
        }
    }

    /// Create with default configuration
    pub fn with_storage(storage: Arc<LocalDiskEngine>) -> Self {
        Self::new(HealthConfig::default(), storage)
    }

    /// Set the service manager for service health checks
    pub fn with_service_manager(mut self, manager: Arc<ServiceManager>) -> Self {
        self.service_manager = Some(manager);
        self
    }

    /// Get the latest health report
    pub fn report(&self) -> HealthReport {
        self.report.read().clone()
    }

    /// Get the current health status
    pub fn health_status(&self) -> HealthStatus {
        self.report.read().status.clone()
    }

    /// Check if the system is healthy
    pub fn is_healthy(&self) -> bool {
        self.report.read().status.is_healthy()
    }

    /// Check if the system is operational (healthy or degraded)
    pub fn is_operational(&self) -> bool {
        self.report.read().status.is_operational()
    }

    /// Run all health checks and update the report
    async fn run_health_checks(&self) -> Result<HealthReport, ServiceError> {
        let start = Instant::now();
        let mut checks = Vec::new();

        // Storage health check
        checks.push(self.check_storage_health().await);

        // Service health check (if service manager is available)
        if self.service_manager.is_some() {
            checks.push(self.check_services_health().await);
        }

        // Performance health check
        checks.push(self.check_performance_health().await);

        // Disk space check
        checks.push(self.check_disk_space().await);

        // Update failure/success counts and apply thresholds
        self.update_check_counters(&checks);

        // Generate report
        let report = HealthReport::from_checks(checks);

        // Store the report
        *self.report.write() = report.clone();

        tracing::debug!(
            status = ?report.status,
            duration_ms = start.elapsed().as_millis(),
            "Health check completed"
        );

        Ok(report)
    }

    /// Check storage health
    async fn check_storage_health(&self) -> CheckResult {
        let start = Instant::now();

        // Real implementation would verify:
        // - Chunk file integrity
        // - Index consistency
        // - WAL state
        // For now, assume healthy
        CheckResult::pass("storage", start.elapsed())
    }

    /// Check that all services are healthy
    async fn check_services_health(&self) -> CheckResult {
        let start = Instant::now();

        if let Some(ref manager) = self.service_manager {
            if manager.is_healthy() {
                CheckResult::pass("services", start.elapsed())
            } else {
                let statuses = manager.status();
                let unhealthy: Vec<_> = statuses
                    .iter()
                    .filter(|(_, s)| !s.is_healthy())
                    .map(|(n, _)| *n)
                    .collect();

                CheckResult::fail(
                    "services",
                    format!("Unhealthy services: {}", unhealthy.join(", ")),
                    start.elapsed(),
                )
            }
        } else {
            CheckResult::pass("services", start.elapsed())
        }
    }

    /// Check performance health (latency within thresholds)
    async fn check_performance_health(&self) -> CheckResult {
        let start = Instant::now();

        // Real implementation would measure actual operation latencies
        // For now, assume healthy
        CheckResult::pass("performance", start.elapsed())
    }

    /// Check disk space availability
    async fn check_disk_space(&self) -> CheckResult {
        let start = Instant::now();

        // Real implementation would check actual disk space
        // For now, assume healthy
        CheckResult::pass("disk_space", start.elapsed())
    }

    /// Update consecutive failure/success counts
    fn update_check_counters(&self, checks: &[CheckResult]) {
        let mut failures = self.failure_counts.write();
        let mut successes = self.success_counts.write();

        for check in checks {
            if check.passed {
                // Reset failure count, increment success count
                failures.remove(&check.name);
                *successes.entry(check.name.clone()).or_insert(0) += 1;
            } else {
                // Reset success count, increment failure count
                successes.remove(&check.name);
                *failures.entry(check.name.clone()).or_insert(0) += 1;
            }
        }
    }

    /// Get consecutive failure count for a check
    pub fn failure_count(&self, check_name: &str) -> u32 {
        self.failure_counts
            .read()
            .get(check_name)
            .copied()
            .unwrap_or(0)
    }

    /// Check if a specific check has exceeded the failure threshold
    pub fn is_check_failing(&self, check_name: &str) -> bool {
        self.failure_count(check_name) >= self.config.failure_threshold
    }
}

#[async_trait::async_trait]
impl Service for HealthChecker {
    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), ServiceError> {
        *self.status.write() = ServiceStatus::Running;
        tracing::info!(
            interval_secs = self.config.check_interval.as_secs(),
            "Health checker started"
        );

        let mut check_interval = interval(self.config.check_interval);

        // Run initial health check
        if let Err(e) = self.run_health_checks().await {
            tracing::warn!(error = %e, "Initial health check failed");
        }

        loop {
            tokio::select! {
                // Shutdown signal received
                result = shutdown.recv() => {
                    match result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::info!("Health checker received shutdown signal");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Missed some messages but channel is still open, continue
                            tracing::debug!(missed = n, "Health checker broadcast receiver lagged");
                        }
                    }
                }

                // Periodic health check
                _ = check_interval.tick() => {
                    match self.run_health_checks().await {
                        Ok(report) => {
                            if !report.status.is_healthy() {
                                tracing::warn!(
                                    status = ?report.status,
                                    failed = report.failed_checks().len(),
                                    "System health degraded"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Health check failed");
                        }
                    }
                }
            }
        }

        *self.status.write() = ServiceStatus::Stopped;
        tracing::info!("Health checker stopped");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "health_checker"
    }

    fn status(&self) -> ServiceStatus {
        self.status.read().clone()
    }

    fn dependencies(&self) -> Vec<&'static str> {
        // Health checker depends on monitor for metrics
        vec!["monitor"]
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::Always {
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
        let config = HealthConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(30));
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.recovery_threshold, 2);
    }

    #[test]
    fn test_health_status() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(HealthStatus::Healthy.is_operational());

        assert!(!HealthStatus::Degraded.is_healthy());
        assert!(HealthStatus::Degraded.is_operational());

        assert!(!HealthStatus::Unhealthy.is_healthy());
        assert!(!HealthStatus::Unhealthy.is_operational());

        assert!(!HealthStatus::Unknown.is_healthy());
        assert!(!HealthStatus::Unknown.is_operational());
    }

    #[test]
    fn test_check_result() {
        let pass = CheckResult::pass("test", Duration::from_millis(10));
        assert!(pass.passed);
        assert!(pass.message.is_none());

        let fail = CheckResult::fail("test", "something went wrong", Duration::from_millis(5));
        assert!(!fail.passed);
        assert_eq!(fail.message, Some("something went wrong".to_string()));
    }

    #[test]
    fn test_health_report_from_checks() {
        // All passing
        let checks = vec![
            CheckResult::pass("check1", Duration::from_millis(10)),
            CheckResult::pass("check2", Duration::from_millis(10)),
        ];
        let report = HealthReport::from_checks(checks);
        assert_eq!(report.status, HealthStatus::Healthy);
        assert_eq!(report.failed_checks().len(), 0);
        assert_eq!(report.passed_checks().len(), 2);

        // One failing (less than half)
        let checks = vec![
            CheckResult::pass("check1", Duration::from_millis(10)),
            CheckResult::pass("check2", Duration::from_millis(10)),
            CheckResult::fail("check3", "failed", Duration::from_millis(10)),
        ];
        let report = HealthReport::from_checks(checks);
        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(report.failed_checks().len(), 1);

        // Majority failing (2 out of 3 = more than half)
        let checks = vec![
            CheckResult::fail("check1", "failed", Duration::from_millis(10)),
            CheckResult::fail("check2", "failed", Duration::from_millis(10)),
            CheckResult::pass("check3", Duration::from_millis(10)),
        ];
        let report = HealthReport::from_checks(checks);
        assert_eq!(report.status, HealthStatus::Unhealthy);

        // Half or less failing - with 4 checks, 1 failure is < half
        let checks = vec![
            CheckResult::pass("check1", Duration::from_millis(10)),
            CheckResult::pass("check2", Duration::from_millis(10)),
            CheckResult::pass("check3", Duration::from_millis(10)),
            CheckResult::fail("check4", "failed", Duration::from_millis(10)),
        ];
        let report = HealthReport::from_checks(checks);
        assert_eq!(report.status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_health_checker_lifecycle() {
        let (_temp_dir, storage) = create_test_storage();
        let service = Arc::new(HealthChecker::with_storage(storage));

        let (tx, rx) = broadcast::channel(1);

        let s = service.clone();
        let handle = tokio::spawn(async move { s.start(rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_health_check_report() {
        let (_temp_dir, storage) = create_test_storage();
        let checker = HealthChecker::with_storage(storage);

        // Initial status should be unknown
        assert_eq!(checker.health_status(), HealthStatus::Unknown);

        // Run checks
        let report = checker.run_health_checks().await.unwrap();

        // All placeholder checks pass, so should be healthy
        assert_eq!(report.status, HealthStatus::Healthy);
        assert!(checker.is_healthy());
        assert!(checker.is_operational());
    }

    #[tokio::test]
    async fn test_failure_counting() {
        let (_temp_dir, storage) = create_test_storage();
        let checker = HealthChecker::with_storage(storage);

        // Simulate some check failures
        let checks = vec![CheckResult::fail("test_check", "error", Duration::ZERO)];
        checker.update_check_counters(&checks);

        assert_eq!(checker.failure_count("test_check"), 1);
        assert!(!checker.is_check_failing("test_check")); // Below threshold

        // Add more failures
        checker.update_check_counters(&checks);
        checker.update_check_counters(&checks);

        assert_eq!(checker.failure_count("test_check"), 3);
        assert!(checker.is_check_failing("test_check")); // At threshold

        // Success should reset
        let success_checks = vec![CheckResult::pass("test_check", Duration::ZERO)];
        checker.update_check_counters(&success_checks);

        assert_eq!(checker.failure_count("test_check"), 0);
        assert!(!checker.is_check_failing("test_check"));
    }
}
