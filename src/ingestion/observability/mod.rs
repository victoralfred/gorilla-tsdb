//! Observability layer for ingestion pipeline
//!
//! Provides unified health checks, metrics exposure, and monitoring
//! capabilities for the ingestion subsystem.
//!
//! # Components
//!
//! - **Health Checks**: Liveness and readiness probes for orchestration
//! - **Prometheus Metrics**: Detailed ingestion metrics in Prometheus format
//! - **Status Reporting**: System status aggregation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                  Observability Layer                     │
//! │  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐ │
//! │  │  Health Check │  │   Prometheus  │  │   Status     │ │
//! │  │   /health     │  │   /metrics    │  │   Reporter   │ │
//! │  └───────┬───────┘  └───────┬───────┘  └──────┬───────┘ │
//! └──────────┼──────────────────┼─────────────────┼─────────┘
//!            │                  │                 │
//!            └──────────────────┴─────────────────┘
//!                              │
//!                    ┌─────────▼─────────┐
//!                    │  IngestionMetrics │
//!                    │   (atomic stats)  │
//!                    └───────────────────┘
//! ```

pub mod health;
pub mod prometheus_exporter;

pub use health::{HealthCheck, HealthStatus, ReadinessCheck, SubsystemHealth};
pub use prometheus_exporter::{PrometheusConfig, PrometheusExporter};
