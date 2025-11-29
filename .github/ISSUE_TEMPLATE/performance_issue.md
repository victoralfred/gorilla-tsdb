---
name: Performance Issue
about: Report performance problems or regressions
title: "[PERF] "
labels: performance
assignees: ''
---

## Performance Issue Description

<!-- Describe the performance problem you're experiencing -->

## Environment

- **OS**: (e.g., Ubuntu 22.04)
- **CPU**: (e.g., AMD Ryzen 9 5900X, 12 cores)
- **RAM**: (e.g., 32GB DDR4)
- **Storage**: (e.g., NVMe SSD, HDD)
- **Rust Version**: (output of `rustc --version`)
- **Gorilla TSDB Version**: (git commit hash or version tag)
- **Build Profile**: (debug/release)

## Workload Characteristics

- **Data points per second**:
- **Number of time series**:
- **Average series cardinality**:
- **Query patterns**: (range queries, aggregations, etc.)

## Expected Performance

<!-- What performance did you expect? -->

## Actual Performance

<!-- What performance are you seeing? Include metrics if available -->

### Benchmark Results

```
<!-- Paste benchmark output here -->
```

### Profiling Data

<!-- If you've done any profiling, share the results -->

## Reproduction Steps

1.
2.
3.

## Configuration

```toml
# Paste your configuration here (remove sensitive data)
```

## Additional Context

<!-- Any other information that might help diagnose the issue -->
