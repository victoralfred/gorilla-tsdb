//! Aggregation Functions and Time Bucketing
//!
//! This module provides time-based aggregation with auto-interval calculation
//! for optimal visualization of time-series data.

use super::types::{TimeAggregationInfo, TimeBucket};
use gorilla_tsdb::query::AggregationFunction as QueryAggFunction;
use gorilla_tsdb::types::{DataPoint, TimeRange};

// =============================================================================
// Constants
// =============================================================================

/// Standard intervals for time bucketing (in milliseconds)
/// These are the "nice" intervals that make sense for visualization
pub const STANDARD_INTERVALS_MS: &[(i64, &str)] = &[
    (1_000, "1s"),
    (10_000, "10s"),
    (30_000, "30s"),
    (60_000, "1m"),
    (300_000, "5m"),
    (900_000, "15m"),
    (1_800_000, "30m"),
    (3_600_000, "1h"),
    (21_600_000, "6h"),
    (43_200_000, "12h"),
    (86_400_000, "1d"),
];

/// Default target number of data points for auto-interval calculation
pub const DEFAULT_TARGET_POINTS: usize = 200;

/// Maximum recommended data points before warning
pub const MAX_RECOMMENDED_POINTS: usize = 300;

/// Minimum recommended data points before warning
pub const MIN_RECOMMENDED_POINTS: usize = 10;

// =============================================================================
// Auto-Interval Calculation
// =============================================================================

/// Calculate the optimal time bucket interval based on the query timeframe
///
/// This function automatically chooses an interval that results in approximately
/// `target_points` data points, then rounds to the nearest standard interval.
///
/// # Arguments
/// * `time_range_ms` - The total time range in milliseconds
/// * `target_points` - Target number of data points (default: 200)
///
/// # Returns
/// * Tuple of (interval_ms, interval_string)
pub fn calculate_auto_interval(time_range_ms: i64, target_points: usize) -> (i64, String) {
    if time_range_ms <= 0 || target_points == 0 {
        return (60_000, "1m".to_string()); // Default to 1 minute
    }

    // Calculate ideal interval
    let ideal_interval_ms = time_range_ms / target_points as i64;

    // Find the nearest standard interval
    let mut best_interval = STANDARD_INTERVALS_MS[0];
    let mut min_diff = i64::MAX;

    for &(interval_ms, label) in STANDARD_INTERVALS_MS {
        let diff = (interval_ms - ideal_interval_ms).abs();
        if diff < min_diff {
            min_diff = diff;
            best_interval = (interval_ms, label);
        }
    }

    (best_interval.0, best_interval.1.to_string())
}

/// Validate a user-specified interval and return warnings if needed
///
/// # Arguments
/// * `interval_ms` - The user-specified interval in milliseconds
/// * `time_range_ms` - The total time range in milliseconds
///
/// # Returns
/// * Optional warning message if the interval may cause issues
pub fn validate_interval(interval_ms: i64, time_range_ms: i64) -> Option<String> {
    if interval_ms <= 0 || time_range_ms <= 0 {
        return None;
    }

    let estimated_points = time_range_ms / interval_ms;

    if estimated_points > MAX_RECOMMENDED_POINTS as i64 {
        Some(format!(
            "The selected interval will produce approximately {} data points, which may impact performance. Consider using 'auto' or a larger interval.",
            estimated_points
        ))
    } else if estimated_points < MIN_RECOMMENDED_POINTS as i64 {
        Some(format!(
            "The selected interval will produce only approximately {} data points, which may not provide enough granularity. Consider using 'auto' or a smaller interval.",
            estimated_points
        ))
    } else {
        None
    }
}

/// Parse a duration string like "5m", "1h", "30s" into milliseconds
///
/// # Supported formats
/// * "Ns" - N seconds
/// * "Nm" - N minutes
/// * "Nh" - N hours
/// * "Nd" - N days
#[allow(dead_code)]
pub fn parse_interval_to_ms(interval: &str) -> Option<i64> {
    let interval = interval.trim().to_lowercase();
    if interval == "auto" {
        return None; // Signal to use auto calculation
    }

    let len = interval.len();
    if len < 2 {
        return None;
    }

    let (num_str, unit) = interval.split_at(len - 1);
    let num: i64 = num_str.parse().ok()?;

    match unit {
        "s" => Some(num * 1_000),
        "m" => Some(num * 60_000),
        "h" => Some(num * 3_600_000),
        "d" => Some(num * 86_400_000),
        _ => None,
    }
}

/// Format milliseconds as a human-readable interval string
pub fn format_interval_ms(ms: i64) -> String {
    if ms >= 86_400_000 && ms % 86_400_000 == 0 {
        format!("{}d", ms / 86_400_000)
    } else if ms >= 3_600_000 && ms % 3_600_000 == 0 {
        format!("{}h", ms / 3_600_000)
    } else if ms >= 60_000 && ms % 60_000 == 0 {
        format!("{}m", ms / 60_000)
    } else {
        format!("{}s", ms / 1_000)
    }
}

// =============================================================================
// Time Bucketing
// =============================================================================

/// Aggregate data points into time buckets
///
/// Groups data points by time windows and applies the specified aggregation
/// function to each bucket. Returns a vector of TimeBucket with one entry
/// per non-empty bucket.
///
/// # Arguments
/// * `points` - Sorted data points to aggregate
/// * `interval_ms` - Bucket size in milliseconds
/// * `time_range` - The full query time range for bucket alignment
/// * `func` - Aggregation function to apply per bucket
///
/// # Returns
/// Vector of `TimeBucket` structs with aggregated values per time window
pub fn aggregate_into_buckets(
    points: &[DataPoint],
    interval_ms: i64,
    time_range: TimeRange,
    func: &QueryAggFunction,
) -> Vec<TimeBucket> {
    use std::collections::BTreeMap;

    if points.is_empty() || interval_ms <= 0 {
        return vec![];
    }

    // Group points by bucket start time
    let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();

    // Align bucket start to the beginning of the time range
    let range_start = time_range.start;

    for point in points {
        // Calculate which bucket this point belongs to
        let bucket_index = (point.timestamp - range_start) / interval_ms;
        let bucket_start = range_start + (bucket_index * interval_ms);

        buckets.entry(bucket_start).or_default().push(point.value);
    }

    // Apply aggregation function to each bucket
    buckets
        .into_iter()
        .map(|(timestamp, values)| {
            let value = aggregate_bucket_values(&values, func);
            TimeBucket { timestamp, value }
        })
        .collect()
}

/// Apply aggregation function to a bucket of values
///
/// Helper function that computes the aggregation for a single time bucket.
pub fn aggregate_bucket_values(values: &[f64], func: &QueryAggFunction) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    match func {
        QueryAggFunction::Count => values.len() as f64,
        QueryAggFunction::Sum => {
            // Kahan summation for numerical stability
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for &v in values {
                let y = v - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            sum
        }
        QueryAggFunction::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
        QueryAggFunction::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        QueryAggFunction::Avg => {
            // Welford's algorithm for numerical stability
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
            }
            mean
        }
        QueryAggFunction::First => values.first().cloned().unwrap_or(f64::NAN),
        QueryAggFunction::Last => values.last().cloned().unwrap_or(f64::NAN),
        QueryAggFunction::StdDev => {
            if values.len() < 2 {
                return 0.0;
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
                let delta2 = v - mean;
                m2 += delta * delta2;
            }
            (m2 / (count - 1) as f64).sqrt()
        }
        QueryAggFunction::Variance => {
            if values.len() < 2 {
                return 0.0;
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for &v in values {
                count += 1;
                let delta = v - mean;
                mean += delta / count as f64;
                let delta2 = v - mean;
                m2 += delta * delta2;
            }
            m2 / (count - 1) as f64
        }
        QueryAggFunction::Median => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = sorted.len() / 2;
            if sorted.len() % 2 == 0 {
                (sorted[mid - 1] + sorted[mid]) / 2.0
            } else {
                sorted[mid]
            }
        }
        // For rate/increase/delta, compute from first and last values
        QueryAggFunction::Rate | QueryAggFunction::Increase | QueryAggFunction::Delta => {
            if values.len() < 2 {
                0.0
            } else {
                let first = values.first().unwrap();
                let last = values.last().unwrap();
                match func {
                    QueryAggFunction::Increase => (last - first).max(0.0),
                    QueryAggFunction::Delta => last - first,
                    QueryAggFunction::Rate => last - first,
                    _ => f64::NAN,
                }
            }
        }
        _ => f64::NAN, // Unsupported in bucket context
    }
}

// =============================================================================
// Scalar Aggregation (for REST API)
// =============================================================================

/// Compute aggregation using function name string (REST API compatibility)
///
/// # Arguments
/// * `function` - Function name as string (count, sum, min, max, avg, etc.)
/// * `points` - Data points to aggregate
///
/// # Returns
/// * Optional aggregated value (None if invalid function or empty points)
pub fn compute_aggregation(function: &str, points: &[DataPoint]) -> Option<f64> {
    if points.is_empty() {
        return None;
    }

    match function.to_lowercase().as_str() {
        "count" => Some(points.len() as f64),
        "sum" => {
            // Kahan summation for numerical stability
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for p in points {
                let y = p.value - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            Some(sum)
        }
        "min" => points
            .iter()
            .map(|p| p.value)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        "max" => points
            .iter()
            .map(|p| p.value)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        "avg" | "mean" => {
            // Welford's online algorithm for numerical stability
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
            }
            Some(mean)
        }
        "first" => points.first().map(|p| p.value),
        "last" => points.last().map(|p| p.value),
        "stddev" | "std" => {
            if points.len() < 2 {
                return Some(0.0);
            }
            // Welford's online algorithm
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            Some((m2 / (count - 1) as f64).sqrt())
        }
        "variance" | "var" => {
            if points.len() < 2 {
                return Some(0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            Some(m2 / (count - 1) as f64)
        }
        _ => None,
    }
}

/// Compute aggregation from AST function enum
///
/// # Arguments
/// * `func` - QueryAggFunction enum
/// * `points` - Data points to aggregate
///
/// # Returns
/// * Tuple of (function_name, aggregated_value)
pub fn compute_aggregation_from_ast(
    func: &QueryAggFunction,
    points: &[DataPoint],
) -> (String, f64) {
    let func_name = format!("{:?}", func).to_lowercase();

    if points.is_empty() {
        return (func_name, f64::NAN);
    }

    let value = match func {
        QueryAggFunction::Count => points.len() as f64,
        QueryAggFunction::Sum => {
            // Kahan summation
            let mut sum = 0.0f64;
            let mut c = 0.0f64;
            for p in points {
                let y = p.value - c;
                let t = sum + y;
                c = (t - sum) - y;
                sum = t;
            }
            sum
        }
        QueryAggFunction::Min => points.iter().map(|p| p.value).fold(f64::INFINITY, f64::min),
        QueryAggFunction::Max => points
            .iter()
            .map(|p| p.value)
            .fold(f64::NEG_INFINITY, f64::max),
        QueryAggFunction::Avg => {
            // Welford's algorithm
            let mut mean = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
            }
            mean
        }
        QueryAggFunction::First => points.first().map(|p| p.value).unwrap_or(f64::NAN),
        QueryAggFunction::Last => points.last().map(|p| p.value).unwrap_or(f64::NAN),
        QueryAggFunction::StdDev => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            (m2 / (count - 1) as f64).sqrt()
        }
        QueryAggFunction::Variance => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let mut count = 0u64;
            for p in points {
                count += 1;
                let delta = p.value - mean;
                mean += delta / count as f64;
                let delta2 = p.value - mean;
                m2 += delta * delta2;
            }
            m2 / (count - 1) as f64
        }
        QueryAggFunction::Median => {
            let mut values: Vec<f64> = points.iter().map(|p| p.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = values.len() / 2;
            if values.len() % 2 == 0 {
                (values[mid - 1] + values[mid]) / 2.0
            } else {
                values[mid]
            }
        }
        // Time-based calculations
        QueryAggFunction::Rate => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let first = points.first().unwrap();
            let last = points.last().unwrap();
            let value_diff = last.value - first.value;
            let time_diff = (last.timestamp - first.timestamp) as f64 / 1000.0;
            if time_diff > 0.0 {
                value_diff / time_diff
            } else {
                0.0
            }
        }
        QueryAggFunction::Increase => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let first = points.first().unwrap();
            let last = points.last().unwrap();
            (last.value - first.value).max(0.0)
        }
        QueryAggFunction::Delta => {
            if points.len() < 2 {
                return (func_name, 0.0);
            }
            let first = points.first().unwrap();
            let last = points.last().unwrap();
            last.value - first.value
        }
        _ => f64::NAN,
    };

    (func_name, value)
}

/// Create TimeAggregationInfo for response
pub fn create_time_aggregation_info(
    mode: String,
    interval_str: String,
    interval_ms: i64,
    bucket_count: usize,
) -> TimeAggregationInfo {
    TimeAggregationInfo {
        mode,
        interval: interval_str,
        interval_ms,
        bucket_count,
        target_points: Some(DEFAULT_TARGET_POINTS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_auto_interval() {
        // 1 hour range should give ~1m buckets for 200 points
        let (interval_ms, label) = calculate_auto_interval(3_600_000, 200);
        assert!((10_000..=60_000).contains(&interval_ms));
        assert!(!label.is_empty());

        // 1 day range should give larger buckets
        let (interval_ms, _) = calculate_auto_interval(86_400_000, 200);
        assert!(interval_ms >= 300_000); // At least 5 minutes
    }

    #[test]
    fn test_validate_interval() {
        // Too few points - 5m interval for 10 mins = 2 points
        let warning = validate_interval(300_000, 600_000);
        assert!(warning.is_some(), "Expected warning for too few points (2)");

        // Too many points - 1s interval for 1 hour = 3600 points
        let warning = validate_interval(1_000, 3_600_000);
        assert!(
            warning.is_some(),
            "Expected warning for too many points (3600)"
        );

        // Good range - 1m interval for 200 mins = 200 points
        let warning = validate_interval(60_000, 12_000_000);
        assert!(warning.is_none(), "Expected no warning for 200 points");
    }

    #[test]
    fn test_format_interval_ms() {
        assert_eq!(format_interval_ms(1_000), "1s");
        assert_eq!(format_interval_ms(60_000), "1m");
        assert_eq!(format_interval_ms(3_600_000), "1h");
        assert_eq!(format_interval_ms(86_400_000), "1d");
    }
}
