---
title: DynamoDbLite write path is slower than read path
type: note
summary: "Writes trail amazon/dynamodb-local; reads beat it. Expression parser is the suspected write-side bottleneck."
tags: [performance, write-path, parser, bench-needed]
created: 2026-05-15
status: evolving
---

# DynamoDbLite write path is slower than read path

Writes trail amazon/dynamodb-local; reads beat it. Expression parser is the suspected write-side bottleneck.

## Observation

From Mark's prior experiment, no formal numbers yet. On the same workload, `DynamoDbLite` is slower than `amazon/dynamodb-local` on writes and faster on reads. The asymmetry is consistent and reproducible across the parity test scenarios.

## Interpretation

Hypothesis: the expression parser is the dominant write-side cost. It runs on every mutating call — `PutItem`, `UpdateItem`, `DeleteItem`, `TransactWriteItems` — for `ConditionExpression` and `UpdateExpression`. The read path touches the parser less; `GetItem` skips it entirely when no `ProjectionExpression` is supplied, and `Query` / `Scan` pay the parser cost once per request, amortized over every result row.

The cross-backend comparison is unmeasured and the parser hypothesis is unconfirmed: a SQLite write-path sweep has since run (`write-path-performance-findings.md`), and it did not measure the parser's share. The read-path win could also come from indexed SQLite lookups beating HTTP-over-loopback round-trips to `amazon/dynamodb-local`.

## Next

Quantify via the planned parity benchmarks project — see [parity-benchmarks-project](parity-benchmarks-project.md). Workloads worth running:

- Bulk `PutItem` (write-heavy, parser-light).
- `PutItem` with `ConditionExpression` (write-heavy, parser-heavy).
- `UpdateItem` with `UpdateExpression` (write-heavy, parser-heavy).
- `GetItem` (read, no expression).
- `Query` with `FilterExpression` (read, parser involved once per request).

If the parser hypothesis holds, the first fix to try is parser-result caching keyed by expression text. The second is a fast lane that skips the parser entirely when no expression is supplied.
