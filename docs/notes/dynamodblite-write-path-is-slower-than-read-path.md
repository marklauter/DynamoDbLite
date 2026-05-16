# DynamoDbLite write path is slower than read path

Tags: performance, write-path, parser, bench-needed
Writes trail amazon/dynamodb-local; reads beat it. Expression parser is the suspected write-side bottleneck.


## Observation

Empirical (Mark's prior experiment, no formal numbers yet): on the same workload, `DynamoDbLite` is materially slower than `amazon/dynamodb-local` on writes, and materially faster on reads. The asymmetry is consistent and reproducible across the parity test workloads.

## Interpretation

Hypothesis: the expression parser is the dominant write-side cost. It runs on every mutating call — `PutItem`, `UpdateItem`, `DeleteItem`, `TransactWriteItems` — for `ConditionExpression` and `UpdateExpression`. The read path touches the parser less; `GetItem` skips it entirely when no `ProjectionExpression` is supplied, and `Query` / `Scan` pay the parser cost once per request, amortized over every result row.

Caveat: this is a hypothesis, not a measurement. The read-path win could equally come from indexed SQLite lookups beating HTTP-over-loopback round-trips to `amazon/dynamodb-local`.

## Next

Quantify via the planned parity benchmarks project — see [parity-benchmarks-project](parity-benchmarks-project.md). Workloads worth running:

- Bulk `PutItem` (write-heavy, parser-light).
- `PutItem` with `ConditionExpression` (write-heavy, parser-heavy).
- `UpdateItem` with `UpdateExpression` (write-heavy, parser-heavy).
- `GetItem` (read, no expression).
- `Query` with `FilterExpression` (read, parser involved once per request).

If the parser hypothesis holds, parser-result caching keyed by expression text — or a fast lane that skips the parser entirely when no expression is supplied — is the ROI target.
