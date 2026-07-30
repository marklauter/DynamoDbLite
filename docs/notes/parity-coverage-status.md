---
title: Parity coverage status
type: note
summary: "Snapshot of which DynamoDB API surfaces have parity coverage today, which are permanently out of scope, and which gaps were investigated and closed."
tags: [parity, status, scope, reference]
created: 2026-05-15
status: evolving
cites: "[[parity-with-dynamodb-local]]"
tracks: "[[implementation-phases]]"
---

# Parity coverage status

Snapshot of which DynamoDB API surfaces have parity coverage today, which are permanently out of scope, and which gaps were investigated and closed.


Reference companion to [[parity-with-dynamodb-local]], which carries the design (container lifecycle, error parity, assertion strategy) and the canonical coverage list.

## What's covered

[[parity-with-dynamodb-local]] carries the canonical list, one entry per parity test class. Maintain it there.

The parity suite under [`tests/DynamoDbLite.Parity.Tests/`](../../tests/DynamoDbLite.Parity.Tests/) runs every scenario against three backends — in-memory SQLite, file-backed SQLite (WAL), and `amazon/dynamodb-local` via Testcontainers — and asserts an explicit AWS-API-contract outcome on each.

## Permanently out of scope

These will not be added regardless of release pressure:

- **Real AWS DynamoDB cloud backend.** Requires credentials, costs money, network-dependent. The three local backends already exercise the contract; the cloud backend would prove the same thing at recurring cost and CI flakiness.
- **TTL parity.** `amazon/dynamodb-local` runs TTL on a long internal cron — expiration windows are minutes-to-hours. That makes CI-friendly cross-backend tests impractical. DynamoDbLite's own TTL behavior is covered in the main test suite; cross-backend equivalence isn't observable without waiting for the container's cron.
- **Export / Import parity.** Export and import are implemented; asserting them across backends is out of scope per [[implementation-phases]]. The semantics are S3-coupled in real DynamoDB; an in-process emulator and `amazon/dynamodb-local` necessarily diverge from S3, so there's nothing meaningful to assert across the three backends.
- **Cross-client response-shape equality.** Replaced by the explicit-expected-outcome strategy. The three clients legitimately differ on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages; a shared bug between two implementations would also pass cross-comparison silently. Each test asserts what the AWS API contract says should happen, not what each client happens to return.

## Gaps that were investigated and closed

Both library gaps surfaced by the parity suite during initial development have been resolved — the parallel-scan one with a residual divergence noted below. No skipped tests remain.

- **Parallel scan ignored `Segment`/`TotalSegments`** — `DynamoDbClient` returned every item in every segment instead of the partition. Fixed by adding stable FNV-1a hashing over the partition key plus a post-store filter (commit `0e435bd`). The post-store filter runs after `Limit`, so the fix closes the no-`Limit` case only; `Limit` combined with `TotalSegments` still diverges — see [[parallel-scan-limit-interaction-gap]].
- **`TransactGetItems` and `BatchGetItem` skipped reserved-word validation on `ProjectionExpression`** — the parser was inside the per-result branch, so empty-result requests bypassed it. Fixed by hoisting the parse out of the result loop so it runs once per request before any store lookup (commit `217beb2`).

## Knobs

`--filter "Backend=DdbLite"` (or `DdbLiteFile` / `DynamoDbLocal`) selects a single backend across the suite via the `Backend` trait emitted by [`BackendDataAttribute`](../../tests/DynamoDbLite.Parity.Tests/Fixtures/BackendDataAttribute.cs). The `amazon/dynamodb-local` container starts lazily, so a lite-only run never spins one up. Full suite ~9s wall, lite-only ~4s.

## Cross-references

- [[parity-with-dynamodb-local]] — design, rationale, and the canonical coverage list.
- [[implementation-phases]] — Phase 14 lineage; out-of-scope justifications.
- [[decide-how-limit-bounds-a-parallel-scan-segment]] — the open question behind the residual parallel-scan divergence.
