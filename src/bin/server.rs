//! Gorilla TSDB HTTP Server
//!
//! This binary provides a complete HTTP server for the Gorilla time-series database.
//! It exposes REST endpoints for data ingestion, querying, and administration.
//!
//! # Endpoints
//!
//! ## Write
//! - `POST /api/v1/write` - Write data points
//!
//! ## Query
//! - `GET /api/v1/query` - Query data points
//! - `POST /api/v1/query` - Query with complex parameters
//!
//! ## Admin
//! - `GET /health` - Health check
//! - `GET /metrics` - Prometheus metrics
//! - `GET /api/v1/stats` - Database statistics
//!
//! # Configuration
//!
//! The server reads configuration from:
//! 1. `TSDB_CONFIG` environment variable (path to TOML file)
//! 2. `./tsdb.toml` in current directory
//! 3. Default configuration
//!
//! # Example
//!
//! ```bash
//! # Start server with default config
//! ./server
//!
//! # Start with custom config
//! TSDB_CONFIG=/etc/tsdb.toml ./server
//!
//! # Write data
//! curl -X POST http://localhost:8080/api/v1/write \
//!   -H "Content-Type: application/json" \
//!   -d '{"series_id": 1, "points": [{"timestamp": 1000, "value": 42.5}]}'
//!
//! # Query data
//! curl "http://localhost:8080/api/v1/query?series_id=1&start=0&end=10000"
//! ```

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use gorilla_tsdb::{
    compression::gorilla::GorillaCompressor,
    engine::{DatabaseConfig, DatabaseStats, InMemoryTimeIndex, TimeSeriesDB, TimeSeriesDBBuilder},
    storage::LocalDiskEngine,
    types::{DataPoint, SeriesId, TagFilter, TimeRange},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::signal;
use tracing::{error, info, warn};

// =============================================================================
// Server Configuration
// =============================================================================

/// Server configuration loaded from TOML or environment
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// HTTP server address
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Data directory path
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Maximum chunk size in bytes
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size: usize,

    /// Data retention in days (None = forever)
    #[serde(default)]
    pub retention_days: Option<u32>,

    /// Enable Prometheus metrics endpoint
    #[serde(default = "default_true")]
    pub enable_metrics: bool,

    /// Maximum points per write request
    #[serde(default = "default_max_write_points")]
    pub max_write_points: usize,
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data/tsdb")
}

fn default_max_chunk_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_true() -> bool {
    true
}

fn default_max_write_points() -> usize {
    100_000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            data_dir: default_data_dir(),
            max_chunk_size: default_max_chunk_size(),
            retention_days: None,
            enable_metrics: true,
            max_write_points: default_max_write_points(),
        }
    }
}

// =============================================================================
// Application State
// =============================================================================

/// Shared application state
struct AppState {
    db: TimeSeriesDB,
    config: ServerConfig,
}

// =============================================================================
// API Request/Response Types
// =============================================================================

/// Write request body
#[derive(Debug, Deserialize)]
struct WriteRequest {
    /// Series ID to write to
    series_id: SeriesId,
    /// Data points to write
    points: Vec<WritePoint>,
}

/// Single point in write request
#[derive(Debug, Deserialize)]
struct WritePoint {
    timestamp: i64,
    value: f64,
}

/// Write response
#[derive(Debug, Serialize)]
struct WriteResponse {
    success: bool,
    points_written: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Query parameters
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Series ID to query
    series_id: SeriesId,
    /// Start timestamp (inclusive)
    start: i64,
    /// End timestamp (inclusive)
    end: i64,
    /// Maximum points to return (default: 10000)
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    10_000
}

/// Query response
#[derive(Debug, Serialize)]
struct QueryResponse {
    success: bool,
    series_id: SeriesId,
    points: Vec<QueryPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Single point in query response
#[derive(Debug, Serialize)]
struct QueryPoint {
    timestamp: i64,
    value: f64,
}

/// Register series request
#[derive(Debug, Deserialize)]
struct RegisterSeriesRequest {
    series_id: SeriesId,
    metric_name: String,
    #[serde(default)]
    tags: HashMap<String, String>,
}

/// Find series request
#[derive(Debug, Deserialize)]
struct FindSeriesParams {
    metric_name: String,
    #[serde(default)]
    tags: Option<HashMap<String, String>>,
}

/// Find series response
#[derive(Debug, Serialize)]
struct FindSeriesResponse {
    success: bool,
    series_ids: Vec<SeriesId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Stats response
#[derive(Debug, Serialize)]
struct StatsResponse {
    total_chunks: u64,
    total_bytes: u64,
    total_series: u64,
    write_ops: u64,
    read_ops: u64,
    compression_ratio: f64,
}

impl From<DatabaseStats> for StatsResponse {
    fn from(stats: DatabaseStats) -> Self {
        Self {
            total_chunks: stats.total_chunks,
            total_bytes: stats.total_bytes,
            total_series: stats.total_series,
            write_ops: stats.write_ops,
            read_ops: stats.read_ops,
            compression_ratio: stats.compression_ratio,
        }
    }
}

/// Health response
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

// =============================================================================
// API Handlers
// =============================================================================

/// Health check endpoint
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Write data points
async fn write_points(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WriteRequest>,
) -> impl IntoResponse {
    // Validate request
    if req.points.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                points_written: 0,
                chunk_id: None,
                error: Some("No points provided".to_string()),
            }),
        );
    }

    if req.points.len() > state.config.max_write_points {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                points_written: 0,
                chunk_id: None,
                error: Some(format!(
                    "Too many points: {} exceeds maximum {}",
                    req.points.len(),
                    state.config.max_write_points
                )),
            }),
        );
    }

    // Convert to DataPoints
    let points: Vec<DataPoint> = req
        .points
        .iter()
        .map(|p| DataPoint::new(req.series_id, p.timestamp, p.value))
        .collect();

    // Write to database
    match state.db.write(req.series_id, points.clone()).await {
        Ok(chunk_id) => (
            StatusCode::OK,
            Json(WriteResponse {
                success: true,
                points_written: points.len(),
                chunk_id: Some(chunk_id.to_string()),
                error: None,
            }),
        ),
        Err(e) => {
            error!(error = %e, series_id = req.series_id, "Write failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WriteResponse {
                    success: false,
                    points_written: 0,
                    chunk_id: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Query data points
async fn query_points(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    // Validate time range
    if params.start > params.end {
        return (
            StatusCode::BAD_REQUEST,
            Json(QueryResponse {
                success: false,
                series_id: params.series_id,
                points: vec![],
                error: Some("start must be <= end".to_string()),
            }),
        );
    }

    let time_range = TimeRange::new_unchecked(params.start, params.end);

    // Query database
    match state.db.query(params.series_id, time_range).await {
        Ok(points) => {
            let response_points: Vec<QueryPoint> = points
                .into_iter()
                .take(params.limit)
                .map(|p| QueryPoint {
                    timestamp: p.timestamp,
                    value: p.value,
                })
                .collect();

            (
                StatusCode::OK,
                Json(QueryResponse {
                    success: true,
                    series_id: params.series_id,
                    points: response_points,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, series_id = params.series_id, "Query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryResponse {
                    success: false,
                    series_id: params.series_id,
                    points: vec![],
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Register a new series
async fn register_series(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeriesRequest>,
) -> impl IntoResponse {
    match state
        .db
        .register_series(req.series_id, &req.metric_name, req.tags)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "series_id": req.series_id
            })),
        ),
        Err(e) => {
            error!(error = %e, series_id = req.series_id, "Register series failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "success": false,
                    "error": e.to_string()
                })),
            )
        }
    }
}

/// Find series by metric name and tags
async fn find_series(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FindSeriesParams>,
) -> impl IntoResponse {
    let tag_filter = match params.tags {
        Some(tags) if !tags.is_empty() => TagFilter::Exact(tags),
        _ => TagFilter::All,
    };

    match state.db.find_series(&params.metric_name, &tag_filter).await {
        Ok(series_ids) => (
            StatusCode::OK,
            Json(FindSeriesResponse {
                success: true,
                series_ids,
                error: None,
            }),
        ),
        Err(e) => {
            error!(error = %e, metric_name = %params.metric_name, "Find series failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FindSeriesResponse {
                    success: false,
                    series_ids: vec![],
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

/// Get database statistics
async fn get_stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    Json(state.db.stats().into())
}

/// Prometheus metrics endpoint (placeholder)
async fn metrics() -> impl IntoResponse {
    // TODO: Implement proper Prometheus metrics export
    "# HELP tsdb_up Indicates if TSDB is up\n# TYPE tsdb_up gauge\ntsdb_up 1\n"
}

// =============================================================================
// Server Initialization
// =============================================================================

/// Load configuration from file or environment
fn load_config() -> ServerConfig {
    // Check environment variable first
    if let Ok(path) = std::env::var("TSDB_CONFIG") {
        match std::fs::read_to_string(&path) {
            Ok(content) => match toml::from_str(&content) {
                Ok(config) => {
                    info!(path = %path, "Loaded configuration from file");
                    return config;
                }
                Err(e) => {
                    warn!(path = %path, error = %e, "Failed to parse config file, using defaults");
                }
            },
            Err(e) => {
                warn!(path = %path, error = %e, "Failed to read config file, using defaults");
            }
        }
    }

    // Check default config file
    if let Ok(content) = std::fs::read_to_string("tsdb.toml") {
        if let Ok(config) = toml::from_str(&content) {
            info!("Loaded configuration from tsdb.toml");
            return config;
        }
    }

    info!("Using default configuration");
    ServerConfig::default()
}

/// Initialize the database
async fn init_database(config: &ServerConfig) -> Result<TimeSeriesDB, Box<dyn std::error::Error>> {
    // Create data directory
    std::fs::create_dir_all(&config.data_dir)?;

    // Create storage engine
    let storage = LocalDiskEngine::new(config.data_dir.clone())?;

    // Create in-memory index (production would use Redis)
    let index = InMemoryTimeIndex::new();

    // Create compressor
    let compressor = GorillaCompressor::new();

    // Build database config
    let db_config = DatabaseConfig {
        data_dir: config.data_dir.clone(),
        redis_url: None,
        max_chunk_size: config.max_chunk_size,
        retention_days: config.retention_days,
        custom_options: HashMap::new(),
    };

    // Build database
    let db = TimeSeriesDBBuilder::new()
        .with_config(db_config)
        .with_compressor(compressor)
        .with_storage(storage)
        .with_index(index)
        .build()
        .await?;

    Ok(db)
}

/// Build the router with all endpoints
fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health and metrics
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        // API v1
        .route("/api/v1/write", post(write_points))
        .route("/api/v1/query", get(query_points))
        .route("/api/v1/series", post(register_series))
        .route("/api/v1/series/find", get(find_series))
        .route("/api/v1/stats", get(get_stats))
        .with_state(state)
}

/// Graceful shutdown handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}

// =============================================================================
// Main Entry Point
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("gorilla_tsdb=info".parse()?)
                .add_directive("server=info".parse()?),
        )
        .init();

    info!("Gorilla TSDB Server starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = load_config();
    info!("Data directory: {:?}", config.data_dir);
    info!("Listen address: {}", config.listen_addr);

    // Initialize database
    info!("Initializing database...");
    let db = init_database(&config).await?;
    info!("Database initialized successfully");

    // Create application state
    let state = Arc::new(AppState {
        db,
        config: config.clone(),
    });

    // Build router
    let app = build_router(state);

    // Parse listen address
    let addr: SocketAddr = config.listen_addr.parse()?;
    info!("Starting HTTP server on {}", addr);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shutdown complete");
    Ok(())
}
