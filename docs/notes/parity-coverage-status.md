# Parity coverage status

Tags: parity, status, scope, reference
Snapshot of which DynamoDB API surfaces have parity coverage today, which are permanently out of scope, and which gaps were investigated and closed.


Reference companion to [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md), which carries the design (container lifecycle, error parity, assertion strategy) and the file-by-file coverage list. This note carries the current-state framing — closed gaps, knobs, and the audit trail.

## What's covered

The parity suite under [`tests/DynamoDbLite.Parity.Tests/`](../../tests/DynamoDbLite.Parity.Tests/) runs every scenario against three backends — in-memory SQLite, file-backed SQLite (WAL), and `amazon/dynamodb-local` via Testcontainers — and asserts an explicit AWS-API-contract outcome on each.

- **Item CRUD**: `PutItem` / `GetItem` round-trip across S/N/BOOL/L/M and B/NULL/SS/NS/BS; `attribute_not_exists` and `attribute_exists` conditions on `PutItem`/`DeleteItem` (success and `ConditionalCheckFailedException`); empty-string scalar values round-trip cleanly.
- **Update expressions**: `SET` with `if_not_exists`, `SET` with `list_append`, `ADD` on number, `REMOVE`, `DELETE` on string set, `size()` in `ConditionExpression`.
- **Query**: `KeyConditionExpression`, `begins_with` on the sort key, `ScanIndexForward = false`, `Limit` + `LastEvaluatedKey` pagination, `BETWEEN` on a numeric sort key, `Select = COUNT`.
- **Scan**: `FilterExpression`, `contains()` on string set, `IN` against a value list, `size()` in `FilterExpression`, parallel scan with `Segment`/`TotalSegments`, `Select = COUNT`.
- **Transactions**: `TransactWriteItems` all-or-nothing rollback with `CancellationReasons[i].Code`, multiple simultaneous condition failures, `ClientRequestToken` idempotency on replay, `ReturnValuesOnConditionCheckFailure = ALL_OLD`; `TransactGetItems` happy path across two tables and missing-key behaviour.
- **Batch**: `BatchGetItem`; `BatchWriteItem` with put + delete in one batch and across two tables.
- **Indexes**: GSI projection variants (`INCLUDE`, `KEYS_ONLY`, `ALL`); LSI query with `begins_with` on the alternate sort key + `INCLUDE` projection.
- **Reserved words**: rejection in `UpdateExpression`/`ConditionExpression`/`ProjectionExpression`; escape via `ExpressionAttributeNames` bypasses the check.
- **Validation order**: malformed expressions are rejected with `ValidationException` *before* any item lookup or mutation across `DeleteItem`, `Query`, `Scan`, `TransactWriteItems`, `TransactGetItems`, and `BatchGetItem`.
- **Return values**: `ALL_OLD` / `NONE` on `PutItem`; `ALL_OLD` / `UPDATED_OLD` / `ALL_NEW` / `UPDATED_NEW` on `UpdateItem`; `ALL_OLD` on `DeleteItem`.

## Permanently out of scope

These will not be added regardless of release pressure. Each has a load-bearing reason that doesn't go away:

- **Real AWS DynamoDB cloud backend.** Requires credentials, costs money, network-dependent. The three local backends already exercise the contract; the cloud backend would prove the same thing at recurring cost and CI flakiness.
- **TTL parity.** `amazon/dynamodb-local` runs TTL on a long internal cron — expiration windows are minutes-to-hours, which makes CI-friendly cross-backend tests impractical. DynamoDbLite's own TTL behaviour is covered in the main test suite; cross-backend equivalence isn't observable without waiting for the container's cron.
- **Export / Import.** Out of scope per [`adrs/index.md`](../adrs/index.md). The semantics are S3-coupled in real DynamoDB; an in-process emulator and `amazon/dynamodb-local` necessarily diverge from S3, so there's nothing meaningful to assert across the three backends.
- **Cross-client response-shape equality.** Replaced by the explicit-expected-outcome strategy. The three clients legitimately differ on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages; a shared bug between two implementations would also pass cross-comparison silently. Each test asserts what the AWS API contract says should happen, not what each client happens to return.

## Gaps that were investigated and closed

Both library gaps surfaced by the parity suite during initial development have been resolved. No skipped tests remain.

- **Parallel scan ignored `Segment`/`TotalSegments`** — `DynamoDbClient` returned every item in every segment instead of the partition. Fixed by adding stable FNV-1a hashing over the partition key plus a post-store filter (commit `0e435bd`).
- **`TransactGetItems` and `BatchGetItem` skipped reserved-word validation on `ProjectionExpression`** — the parser was inside the per-result branch, so empty-result requests bypassed it. Fixed by hoisting the parse out of the result loop so it runs once per request before any store lookup (commit `217beb2`).

## Knobs

`--filter "Backend=DdbLite"` (or `DdbLiteFile` / `DynamoDbLocal`) selects a single backend across the suite via the `Backend` trait emitted by [`BackendDataAttribute`](../../tests/DynamoDbLite.Parity.Tests/Fixtures/BackendDataAttribute.cs). The `amazon/dynamodb-local` container starts lazily, so a lite-only run never spins one up. Full suite ~9s wall, lite-only ~4s.

## Cross-references

- [`parity-with-dynamodb-local.md`](parity-with-dynamodb-local.md) — design, rationale, and full coverage list.
- [`docs/adrs/index.md`](../adrs/index.md) — Phase 14 lineage; out-of-scope justifications.
