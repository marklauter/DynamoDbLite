# Parity coverage gaps in operation variants

Tags: todo,parity,gap,v1.1
Today's parity suite samples the contract surface but doesn't exhaust it; six operation areas have known missing variants where backend drift could land silently between releases.

## Observation

The parity suite under [`tests/DynamoDbLite.Parity.Tests/`](../../tests/DynamoDbLite.Parity.Tests/) passes 192/192 tests against three backends (DdbLite, DdbLiteFile, DynamoDbLocal) and gives 57% line / 46% branch coverage of the library. Per-suite coverage being lower than the main suite (93% / 81%) is partly structural — TTL, Export/Import, ORM, and out-of-scope operations are intentionally excluded — but partly variant-coverage gaps where adding scenarios would catch real cross-backend drift.

Covered list is in [parity-with-dynamodb-local](parity-with-dynamodb-local.md); permanent out-of-scope items are in [parity-coverage-status](parity-coverage-status.md). What follows is the non-structural gap: scenarios that *are* in scope and *do* have AWS-API-observable behavior, but aren't yet asserted across the three backends.

### Update expressions

Today covers: `SET if_not_exists`, `SET list_append`, `ADD` on number, `REMOVE` single, `DELETE` on string set.

Missing:

- Nested map updates (`SET map.field = :v`).
- List-index assignment (`SET list[0] = :v`).
- Multiple `SET` clauses in one expression (`SET a = :a, b = :b`).
- `ADD` on number set (today only number scalar).
- `ADD` on string set.
- `REMOVE` of multiple paths in one expression.
- `DELETE` on number set.

### Condition expressions

Today covers: `attribute_exists`, `attribute_not_exists`, `size()`.

Missing:

- `attribute_type` function (`attribute_type(field, :type)`).
- `contains` on string (today's `contains` parity is on string-set only).
- `contains` on list.
- `begins_with` in a `ConditionExpression` (today's `begins_with` parity is on sort-key conditions, which is a different parser path).
- Complex AND/OR/NOT trees (today's conditions are single-clause).
- Deep nesting (`((a AND b) OR (c AND d))`).

### Query

Today covers: `KeyConditionExpression` basics, `begins_with` on sort key, `ScanIndexForward = false`, `Limit` + `LastEvaluatedKey` pagination, `BETWEEN` on numeric sort key.

Missing:

- `BETWEEN` on **string** sort key (only numeric is tested; lexicographic vs numeric path differs in the SQL builder).
- Multiple AND clauses in a single `KeyConditionExpression` (e.g., `pk = :p AND sk BETWEEN :a AND :b`).
- `ConsistentRead = true` on table query (asserts behavior, not just acceptance).
- `ConsistentRead = true` rejected on GSI query (`AmazonDynamoDBException`).

### Scan

Today covers: `FilterExpression`, `contains` on string set, `IN` against a value list, `size()` in `FilterExpression`, parallel scan with `Segment` / `TotalSegments`.

Missing:

- Nested map filtering (`FilterExpression = "map.field = :v"`).
- List `size()` filtering (today's `size()` is on string).
- Multiple OR clauses (today's filters are single-clause AND-only).
- Legacy `ScanFilter` path under cross-backend stress (legacy `Dictionary<string, Condition>` shape; backends may diverge on equivalence with `FilterExpression`).
- Legacy `AttributesToGet` path (legacy projection shape).

### Batch

Today covers: `BatchGetItem` happy path, `BatchWriteItem` put + delete in a single batch, `BatchWriteItem` across two tables.

Missing:

- Partial failures: some items succeed, others land in `UnprocessedItems`.
- `UnprocessedItems` behavior: shape of the dictionary, retry semantics across backends.
- Mixed put + delete with conditional failures (real DynamoDB doesn't support conditions on `BatchWriteItem` — assert all three backends reject identically).
- Oversize batches: >25 items on `BatchWriteItem`, >100 items on `BatchGetItem`. Assert `ValidationException`.

### Transactions

Today covers: `TransactWriteItems` all-or-nothing rollback with `CancellationReasons[i].Code`, multiple simultaneous condition failures, `ClientRequestToken` idempotency, `ReturnValuesOnConditionCheckFailure = ALL_OLD`. `TransactGetItems` happy path across two tables.

Missing:

- Mixed `Put` / `Update` / `Delete` / `ConditionCheck` in one transaction (today's transact scenarios are single-action-type).
- `>100` items rejected with `ValidationException` (DynamoDB's transaction item cap).
- `ReturnConsumedCapacity` shape (`TOTAL` vs `INDEXES` vs `NONE` — backends may diverge on the field's presence).

## Interpretation

The 192 tests today were the v1.0 floor: every major API works the same on all three backends. Variant coverage is the next layer — operators, projections, and error paths that *might* drift between backends but haven't been asked about yet. Absence of evidence isn't evidence of absence; parity hasn't observed drift on `BETWEEN` against a string sort key because parity hasn't tested it.

If we're shipping a library that consumers expect to be drop-in for real DynamoDB, the variant coverage matters: a consumer writing a `SET map.field = :v` update expression today gets the same answer from DdbLite as from real DynamoDB *empirically* (the main suite tests it), but not *parity-asserted* (no cross-backend confirmation). That gap is acceptable for v1.0; it's worth closing for v1.1.

## Next

Sequenced from highest value (most likely place for silent drift) to lowest:

1. **Update expression variants** — the parser is the most divergence-prone surface across backends. Add tests for nested map updates, list-index assignment, multiple SET clauses, ADD on number-set / string-set, REMOVE multiple, DELETE number-set.

2. **Condition expression variants** — same parser-divergence risk. Add `attribute_type`, `contains` on string, `contains` on list, `begins_with` in ConditionExpression (separate path from key-condition), complex AND/OR/NOT trees.

3. **Query and Scan variants** — `BETWEEN` on string sort key, multi-clause KeyConditions, `ConsistentRead` on table and rejection on GSI, nested map filtering, multi-OR filters, legacy `ScanFilter` / `AttributesToGet`.

4. **Batch failure-mode parity** — `UnprocessedItems`, partial failures, oversize batches, condition rejection on `BatchWriteItem`.

5. **Transaction shape parity** — mixed-action transactions, oversize transactions, `ReturnConsumedCapacity` shape.

When the expansion lands, update [parity-with-dynamodb-local](parity-with-dynamodb-local.md)'s Covered list per scenario and recalculate the coverage delta in [parity-coverage-status](parity-coverage-status.md).
