---
title: Export and import get the minimal fix only
type: decision
summary: "Export and import exist so production code calling them keeps running, never as real features; their pagination cursor gets the one-line fix that stops data loss and nothing more."
tags: [scope, api-surface, exports, imports, pagination]
created: 2026-07-31
status: locked
constrained-by: "[[behavioral-fidelity]]"
cites: "[[list-exports-imports-pagination-direction]]"
---

# Export and import get the minimal fix only

Export and import exist so production code that calls `ExportTableToPointInTimeAsync` or `ImportTableAsync` keeps running under DynamoDbLite instead of throwing. They will never gain real capability.

The `ListExports` and `ListImports` continuation cursor gets one change. It was `ROWID > (token's ROWID)` against a query sorting `ORDER BY start_time DESC` — an ascending cursor under a descending sort, which is the data loss described in [ListExports / ListImports pagination filter direction](../notes/list-exports-imports-pagination-direction.md). It becomes `start_time < (token's start_time)`, so the cursor descends on the same column the query sorts on. Nothing else about these operations changes.

Records sharing a `start_time` can still be skipped. The comparison is strictly less-than, and the timestamp does not identify a row.

No single-column cursor fixes that. Comparing `ROWID` instead was measured across every tie position: it skips in the same positions and duplicates in the rest. Uniqueness of the cursor column buys nothing when the sort is on a different column, because within a tie group the rows arrive in scan order against the descending sort. `start_time` is the weakly better of the two.

Exactly-once delivery holds when timestamps are distinct. That is the normal case, since they carry tick resolution and each record is written with file I/O in between.

That limitation stays open.

## Alternatives

**Design a composite `(start_time, arn)` continuation token.** This is the thorough fix. It closes the tie case as well as the direction bug, because the token would identify the same columns the query sorts on rather than an unrelated one.

It lost because it designs ordering semantics for operations that will never do real work. It also cannot be verified the way the rest of the pagination work is: export and import are excluded from the parity suite, so there is no DynamoDB Local to check the answer against. Every other pagination decision is settled by running the reference implementation. This one would be settled by argument.

**Leave the cursor as it is and keep documenting it.** The defect predates the paginator surface and was already recorded as a known limitation.

It lost because the paginator makes the broken path the idiomatic one. `Paginators.ListExports(request)` is how the AWS SDK teaches callers to read a list operation, and enumerating it silently returns a subset with duplicates — five records become three, one of them twice. A known limitation nobody reaches is cheap; one sitting behind the recommended API is not.

**Implement export and import for real.** Users ask for this: pull a chunk of a production table into a local store, or save an experiment.

It lost because the AWS-shaped API is the wrong vehicle for that want. `ExportTableToPointInTime` writes DynamoDB-JSON to S3 and `ImportTable` reads it back; the shape assumes S3, IAM, and an async job the caller polls. Someone seeding a local store from a live table wants none of that — they want a scan against the source and a batch write into the target, which the existing API already provides. Building the S3-shaped machinery would serve the reference implementation rather than the user.

## Why

The cost is a permanent rough edge. Export and import will always be shallower than the operations around them, and the `start_time` tie case stays unresolved. Anyone reading the code will find operations that work but were deliberately not finished, which reads as neglect unless they find this note.

What it buys is a rule that settles the next question without reopening the trade-off: these operations exist so calling code keeps running, and that is all. The rule covers enhancements nobody has proposed yet.

It also keeps the pagination work verifiable. Every other part of that work is checked against the reference implementation, and the composite token could not have been.

The flip itself is not a design choice. The cursor and the sort contradict each other; one of them is wrong, and the sort is load-bearing. Making them agree repairs an internal inconsistency rather than introducing new behavior, which is why it is in scope while the composite token is not.

## See also

- [Out-of-scope operations](out-of-scope-operations.md) — the operations that throw rather than stub, and why. Export and import are not among them; they are implemented, just never deepened.
- [Behavioral fidelity](behavioral-fidelity.md) — the principle that decides how closely an operation must match real DynamoDB.
