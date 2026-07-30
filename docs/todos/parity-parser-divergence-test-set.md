---
title: Parser-divergence parity test set
type: todo
summary: "Six parity-framework-testable gaps on parser or semantic surfaces where the SQLite expression translation could silently diverge from real DynamoDB; each is broken out as an atomic todo for sequenced delivery."
tags: [epic, parity, v1.1]
created: 2026-05-27
priority: medium
status: open
refines: "[[parity-coverage-gaps-in-operation-variants]]"
cites: "[[parity-coverage-status]]"
---

# Parser-divergence parity test set

Six parity-framework-testable gaps where the SQLite expression translation could silently diverge from real DynamoDB; each is broken out as an atomic todo for sequenced delivery.

## Observation

The parity suite proves the major API shapes agree across DdbLite, DdbLiteFile, and DynamoDbLocal. The audit on 2026-05-27 surfaced six gaps that sit on parser or semantic surfaces. A divergence there escapes both the in-process main suite and the broader gap inventory in [[parity-coverage-gaps-in-operation-variants]].

The narrowing criterion: only items that (a) can be exercised against `amazon/dynamodb-local` and (b) carry real parser-divergence risk. TTL is excluded because dynamodb-local's TTL cron makes cross-backend timing impractical (see [[parity-coverage-status]]); ORM and Export/Import are excluded by design. Tags were dropped on review because the operation is a flat string-KV with no parser, no expression, and no ordering — zero divergence surface.

## The six

Sequenced from highest divergence risk to lowest:

1. [[parity-test-condition-expression-breadth]] — `attribute_type`, `contains` on string/list, `begins_with` inside `ConditionExpression`, AND/OR/NOT trees.
2. [[parity-test-update-expression-nesting]] — nested map paths, list-index assignment, multi-clause `SET`, `ADD`/`DELETE` on number/string sets.
3. [[parity-test-scan-pagination]] — `ExclusiveStartKey`/`LastEvaluatedKey` round-trip on `Scan` (Query has it, Scan doesn't).
4. [[parity-test-query-between-string-sort-key]] — lexicographic `BETWEEN` path (numeric is covered).
5. [[parity-test-transaction-mixed-actions]] — `Put` + `Update` + `Delete` + `ConditionCheck` in one `TransactWriteItems`; >100-item rejection.
6. [[parity-test-batch-failure-modes]] — condition-check rejection inside `BatchWriteItem`, oversize batch rejection (>25 / >100).

## Interpretation

This set refines the broader v1.1 gap inventory down to the corner cases where the SQLite expression parser and the DynamoDB grammar are most likely to disagree. The v1.0 floor proved the common-path shapes. Closing this set extends the drop-in claim to expression-heavy workloads.

The broader inventory in [[parity-coverage-gaps-in-operation-variants]] carries the rest: `ConsistentRead`, `ReturnConsumedCapacity`, legacy `ScanFilter` / `AttributesToGet`, multi-OR filter clauses, nested map filtering. Those are real gaps, lower-risk than the parser surface.

## Next

Work the six in listed order. Each child note carries its own acceptance criteria. When the set closes, update [[parity-coverage-status]] Covered list and recompute the line/branch delta against the parity suite.
