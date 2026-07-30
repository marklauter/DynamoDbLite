---
title: Parity benchmarks project
type: note
summary: "BenchmarkDotNet project comparing DdbLite / DdbLiteFile / amazon/dynamodb-local on the same workloads; quantifies the perf asymmetry and gates future optimization."
tags: [release, v1.1, performance, bench]
created: 2026-05-15
status: evolving
measures: "[[dynamodblite-write-path-is-slower-than-read-path]]"
---

# Parity benchmarks project

BenchmarkDotNet project comparing DdbLite / DdbLiteFile / amazon/dynamodb-local on the same workloads; quantifies the perf asymmetry and gates future optimization.

## Observation

No benchmark project exists yet. The perf hypothesis in [dynamodblite-write-path-is-slower-than-read-path](dynamodblite-write-path-is-slower-than-read-path.md) is unmeasured; the indexes shipped in commit `6c6a8ca` are unmeasured; the parallel-scan fix is unmeasured.

## Interpretation

Without numbers, perf claims like "ddblite is faster on reads" are guesses, and so is any perf "optimization". The bench project is what validates the perf-asymmetry hypothesis and the parser-caching opportunity in [parser-result-caching](parser-result-caching.md). It also catches regressions in the SQLite store as the schema evolves.

## Next

Create `tests/DynamoDbLite.Parity.Benchmarks/` with BenchmarkDotNet. Mirror the `ParityBackend` enum (DdbLite / DdbLiteFile / DynamoDbLocal) as benchmark parameter axis. Coverage to include:

- `GetItem` single-item read (parser-free baseline).
- `PutItem` single-item write (parser-free baseline).
- `PutItem` with `ConditionExpression` (parser-heavy write).
- `UpdateItem` with `UpdateExpression` (parser-heavy write).
- `Query` with `FilterExpression` (parser involved once per request, amortized).
- Bulk write of 100 items (write throughput, parser-light vs parser-heavy split).
- Parallel `Scan` with `TotalSegments` against a large table.

Report mean, allocations per op. Add a `pack` target that publishes a benchmark summary alongside the package, or paste results into `CHANGELOG.md`.

When the bench exists, use it to decide whether [parser-result-caching](parser-result-caching.md) is worth implementing.
