//! HTTP Handlers for the Gorilla TSDB Server
//!
//! This module contains all HTTP endpoint handlers for the REST API.

use super::aggregation::compute_aggregation;
use super::config::ServerConfig;
use super::query_router;
use super::types::*;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use gorilla_tsdb::engine::{DatabaseStats, TimeSeriesDB};
use gorilla_tsdb::query::subscription::SubscriptionManager;
use gorilla_tsdb::storage::LocalDiskEngine;
use gorilla_tsdb::types::{DataPoint, SeriesId, TagFilter, TimeRange};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

// =============================================================================
// Application State
// =============================================================================

/// Shared application state containing database, storage, and configuration.
/// The subscriptions field is reserved for future real-time subscription features.
#[allow(dead_code)]
pub struct AppState {
    pub db: TimeSeriesDB,
    pub storage: Arc<LocalDiskEngine>,
    pub config: ServerConfig,
    pub subscriptions: Arc<SubscriptionManager>,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Generate a deterministic series ID from metric name and tags
pub fn generate_series_id(metric: &str, tags: &HashMap<String, String>) -> SeriesId {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};

    let sorted_tags: BTreeMap<_, _> = tags.iter().collect();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metric.hash(&mut hasher);
    for (k, v) in sorted_tags {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }

    hasher.finish() as SeriesId
}

// =============================================================================
// Health & Stats Handlers
// =============================================================================

/// Health check endpoint
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Get database statistics
pub async fn get_stats(State(state): State<Arc<AppState>>) -> Json<StatsResponse> {
    let stats = state.db.stats();
    Json(stats.into())
}

/// Prometheus metrics endpoint
pub async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.db.stats();
    let metrics = format!(
        "# HELP tsdb_total_chunks Total number of chunks\n\
         # TYPE tsdb_total_chunks gauge\n\
         tsdb_total_chunks {}\n\
         # HELP tsdb_total_bytes Total bytes stored\n\
         # TYPE tsdb_total_bytes gauge\n\
         tsdb_total_bytes {}\n\
         # HELP tsdb_total_series Total number of series\n\
         # TYPE tsdb_total_series gauge\n\
         tsdb_total_series {}\n\
         # HELP tsdb_write_ops_total Total write operations\n\
         # TYPE tsdb_write_ops_total counter\n\
         tsdb_write_ops_total {}\n\
         # HELP tsdb_read_ops_total Total read operations\n\
         # TYPE tsdb_read_ops_total counter\n\
         tsdb_read_ops_total {}\n\
         # HELP tsdb_compression_ratio Current compression ratio\n\
         # TYPE tsdb_compression_ratio gauge\n\
         tsdb_compression_ratio {}\n\
         # HELP tsdb_index_cache_hits Index cache hits\n\
         # TYPE tsdb_index_cache_hits counter\n\
         tsdb_index_cache_hits {}\n\
         # HELP tsdb_index_cache_misses Index cache misses\n\
         # TYPE tsdb_index_cache_misses counter\n\
         tsdb_index_cache_misses {}\n\
         # HELP tsdb_index_queries_served Total index queries served\n\
         # TYPE tsdb_index_queries_served counter\n\
         tsdb_index_queries_served {}\n",
        stats.total_chunks,
        stats.total_bytes,
        stats.total_series,
        stats.write_ops,
        stats.read_ops,
        stats.compression_ratio,
        stats.index_cache_hits,
        stats.index_cache_misses,
        stats.index_queries_served,
    );
    (StatusCode::OK, [("content-type", "text/plain")], metrics)
}

impl From<DatabaseStats> for StatsResponse {
    fn from(stats: DatabaseStats) -> Self {
        let total_cache_ops = stats.index_cache_hits + stats.index_cache_misses;
        let cache_hit_rate = if total_cache_ops > 0 {
            (stats.index_cache_hits as f64 / total_cache_ops as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_chunks: stats.total_chunks,
            total_bytes: stats.total_bytes,
            total_series: stats.total_series,
            write_ops: stats.write_ops,
            read_ops: stats.read_ops,
            compression_ratio: stats.compression_ratio,
            index_cache_hits: stats.index_cache_hits,
            index_cache_misses: stats.index_cache_misses,
            index_queries_served: stats.index_queries_served,
            index_cache_hit_rate: cache_hit_rate,
        }
    }
}

// =============================================================================
// Write Handlers
// =============================================================================

/// Write data points
pub async fn write_points(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WriteRequest>,
) -> impl IntoResponse {
    if req.points.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(WriteResponse {
                success: false,
                series_id: None,
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
                series_id: None,
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

    // Determine series ID
    let series_id = match req.series_id {
        Some(id) => id,
        None => {
            match &req.metric {
                Some(metric) => {
                    let id = generate_series_id(metric, &req.tags);

                    // Auto-register the series
                    if let Err(e) = state
                        .storage
                        .register_series_metadata(
                            id,
                            metric,
                            req.tags.clone(),
                            state.config.retention_days,
                        )
                        .await
                    {
                        warn!(error = %e, metric = %metric, "Series metadata persistence");
                    }

                    if let Err(e) = state.db.register_series(id, metric, req.tags.clone()).await {
                        warn!(error = %e, metric = %metric, "Series index registration");
                    }

                    id
                }
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(WriteResponse {
                            success: false,
                            series_id: None,
                            points_written: 0,
                            chunk_id: None,
                            error: Some(
                                "Must provide either 'series_id' or 'metric' name".to_string(),
                            ),
                        }),
                    );
                }
            }
        }
    };

    let points: Vec<DataPoint> = req
        .points
        .iter()
        .map(|p| DataPoint::new(series_id, p.timestamp, p.value))
        .collect();

    match state.db.write(series_id, points.clone()).await {
        Ok(chunk_id) => (
            StatusCode::OK,
            Json(WriteResponse {
                success: true,
                series_id: Some(series_id),
                points_written: points.len(),
                chunk_id: Some(chunk_id.to_string()),
                error: None,
            }),
        ),
        Err(e) => {
            error!(error = %e, series_id = series_id, "Write failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WriteResponse {
                    success: false,
                    series_id: Some(series_id),
                    points_written: 0,
                    chunk_id: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

// =============================================================================
// Query Handlers
// =============================================================================

/// Query data points (REST API)
pub async fn query_points(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let time_range = TimeRange::new_unchecked(params.start, params.end);

    // Determine series ID
    let series_id: SeriesId = match params.series_id {
        Some(id_str) => match id_str.parse::<u128>() {
            Ok(id) => id,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(QueryResponse {
                        success: false,
                        series_id: None,
                        points: vec![],
                        aggregation: None,
                        error: Some(format!("Invalid series_id format: {}", id_str)),
                    }),
                );
            }
        },
        None => match &params.metric {
            Some(metric) => {
                let tags: HashMap<String, String> = params
                    .tags
                    .as_ref()
                    .and_then(|t| serde_json::from_str(t).ok())
                    .unwrap_or_default();

                let tag_filter = if tags.is_empty() {
                    TagFilter::All
                } else {
                    TagFilter::Exact(tags.clone())
                };

                match state.db.find_series(metric, &tag_filter).await {
                    Ok(series_ids) if !series_ids.is_empty() => series_ids[0],
                    Ok(_) => {
                        return (
                            StatusCode::NOT_FOUND,
                            Json(QueryResponse {
                                success: false,
                                series_id: None,
                                points: vec![],
                                aggregation: None,
                                error: Some(format!("Series not found for metric: {}", metric)),
                            }),
                        );
                    }
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(QueryResponse {
                                success: false,
                                series_id: None,
                                points: vec![],
                                aggregation: None,
                                error: Some(format!("Series lookup error: {}", e)),
                            }),
                        );
                    }
                }
            }
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(QueryResponse {
                        success: false,
                        series_id: None,
                        points: vec![],
                        aggregation: None,
                        error: Some("Must provide either 'series_id' or 'metric' name".to_string()),
                    }),
                );
            }
        },
    };

    match state.db.query(series_id, time_range).await {
        Ok(mut points) => {
            points.sort_by_key(|p| p.timestamp);
            let total_points = points.len();
            points.truncate(params.limit);

            // Apply aggregation if requested
            if let Some(ref agg_func) = params.aggregation {
                if let Some(agg_value) = compute_aggregation(agg_func, &points) {
                    return (
                        StatusCode::OK,
                        Json(QueryResponse {
                            success: true,
                            series_id: Some(series_id),
                            points: vec![],
                            aggregation: Some(AggregationResult {
                                function: agg_func.clone(),
                                value: agg_value,
                                point_count: total_points,
                                groups: None,
                            }),
                            error: None,
                        }),
                    );
                }
            }

            let response_points: Vec<QueryPoint> = points
                .iter()
                .map(|p| QueryPoint {
                    timestamp: p.timestamp,
                    value: p.value,
                    series_id: None,
                    tags: None,
                })
                .collect();

            (
                StatusCode::OK,
                Json(QueryResponse {
                    success: true,
                    series_id: Some(series_id),
                    points: response_points,
                    aggregation: None,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, series_id = series_id, "Query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryResponse {
                    success: false,
                    series_id: Some(series_id),
                    points: vec![],
                    aggregation: None,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

// =============================================================================
// SQL/PromQL Query Handler
// =============================================================================

/// Execute SQL or PromQL query
pub async fn execute_sql_promql_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlPromqlRequest>,
) -> impl IntoResponse {
    info!(query = %req.query, language = %req.language, "Executing SQL/PromQL query");

    match query_router::execute_query(&state.db, &req.query, &req.language).await {
        Ok((language, query_type, _points, agg_execution_result, series_data)) => {
            let (from_date, to_date) = series_data
                .as_ref()
                .map(|series| {
                    let mut min_ts = i64::MAX;
                    let mut max_ts = i64::MIN;
                    for s in series {
                        for p in &s.pointlist {
                            let ts = p[0] as i64;
                            min_ts = min_ts.min(ts);
                            max_ts = max_ts.max(ts);
                        }
                    }
                    if min_ts == i64::MAX {
                        (None, None)
                    } else {
                        (Some(min_ts), Some(max_ts))
                    }
                })
                .unwrap_or((None, None));

            let agg_result = agg_execution_result.map(|exec_result| {
                let point_count = exec_result
                    .groups
                    .as_ref()
                    .map(|g| g.iter().map(|gr| gr.point_count).sum())
                    .unwrap_or_else(|| {
                        series_data
                            .as_ref()
                            .map(|s| s.iter().map(|sd| sd.length).sum())
                            .unwrap_or(0)
                    });

                SqlAggregationResult {
                    function: exec_result.func_name,
                    value: Some(exec_result.value),
                    point_count,
                    buckets: exec_result.buckets,
                    groups: exec_result.groups,
                    time_aggregation: exec_result.time_aggregation,
                    warnings: exec_result.warnings,
                }
            });

            (
                StatusCode::OK,
                Json(SqlPromqlResponse {
                    status: "ok".to_string(),
                    series: series_data,
                    from_date,
                    to_date,
                    query_type: Some(query_type),
                    language: Some(language.to_string()),
                    aggregation: agg_result,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, "Query execution failed");
            (
                StatusCode::BAD_REQUEST,
                Json(SqlPromqlResponse {
                    status: "error".to_string(),
                    series: None,
                    from_date: None,
                    to_date: None,
                    query_type: None,
                    language: None,
                    aggregation: None,
                    error: Some(e),
                }),
            )
        }
    }
}

// =============================================================================
// Series Management Handlers
// =============================================================================

/// Register a new series
pub async fn register_series(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeriesRequest>,
) -> impl IntoResponse {
    let series_id = req
        .series_id
        .unwrap_or_else(|| generate_series_id(&req.metric_name, &req.tags));

    // Persist to storage
    if let Err(e) = state
        .storage
        .register_series_metadata(
            series_id,
            &req.metric_name,
            req.tags.clone(),
            state.config.retention_days,
        )
        .await
    {
        warn!(error = %e, metric = %req.metric_name, "Series metadata persistence");
    }

    match state
        .db
        .register_series(series_id, &req.metric_name, req.tags)
        .await
    {
        Ok(()) => {
            info!(series_id = series_id, metric = %req.metric_name, "Series registered");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "series_id": series_id
                })),
            )
        }
        Err(e) => {
            error!(error = %e, "Failed to register series");
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
pub async fn find_series(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FindSeriesParams>,
) -> impl IntoResponse {
    let tag_filter = params.tags.map(TagFilter::Exact).unwrap_or(TagFilter::All);

    match state.db.find_series(&params.metric_name, &tag_filter).await {
        Ok(series_ids) => {
            info!(metric = %params.metric_name, count = series_ids.len(), "Found series");
            (
                StatusCode::OK,
                Json(FindSeriesResponse {
                    success: true,
                    series_ids,
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(error = %e, "Failed to find series");
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
