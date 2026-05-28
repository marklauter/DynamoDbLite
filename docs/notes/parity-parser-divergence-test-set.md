---
title: Parser-divergence parity test set
summary: Six parity-framework-testable gaps on parser or semantic surfaces where the SQLite expression translation could silently diverge from real DynamoDB; each is broken out as an atomic todo for sequenced delivery.
tags: [note, todo, epic, parity, v1.1]
created: 2026-05-27
---

# Parser-divergence parity test set

Six parity-framework-testable gaps where the SQLite expression translation could silently diverge from real DynamoDB; each is broken out as an atomic todo for sequenced delivery.

## Observation

The parity suite proves the major API shapes agree across DdbLite, DdbLiteFile, and DynamoDbLocal, but the audit on 2026-05-27 surfaced six gaps that sit specifically on parser or semantic surfaces where a divergence would not be caught by the in-process main suite or the broader gap inventory in [[docs/notes/parity-coverage-gaps-in-operation-variants.md]].

The narrowing criterion: only items that (a) can be exercised against `amazon/dynamodb-local` and (b) carry real parser-divergence risk. TTL is excluded because dynamodb-local's TTL cron makes cross-backend timing impractical (see [[docs/notes/parity-coverage-status.md]]); ORM and Export/Import are excluded by design. Tags were dropped on review because the operation is a flat string-KV with no parser, no expression, and no ordering — zero divergence surface.

## The six

Sequenced from highest divergence risk to lowest:

1. [[docs/notes/parity-test-condition-expression-breadth.md]] — `attribute_type`, `contains` on string/list, `begins_with` inside `ConditionExpression`, AND/OR/NOT trees.
2. [[docs/notes/parity-test-update-expression-nesting.md]] — nested map paths, list-index assignment, multi-clause `SET`, `ADD`/`DELETE` on number/string sets.
3. [[docs/notes/parity-test-scan-pagination.md]] — `ExclusiveStartKey`/`LastEvaluatedKey` round-trip on `Scan` (Query has it, Scan doesn't).
4. [[docs/notes/parity-test-query-between-string-sort-key.md]] — lexicographic `BETWEEN` path (numeric is covered).
5. [[docs/notes/parity-test-transaction-mixed-actions.md]] — `Put` + `Update` + `Delete` + `ConditionCheck` in one `TransactWriteItems`; >100-item rejection.
6. [[docs/notes/parity-test-batch-failure-modes.md]] — condition-check rejection inside `BatchWriteItem`, oversize batch rejection (>25 / >100).

## Interpretation

This set is a refinement of the broader v1.1 gap inventory, focused on places where the SQLite expression parser and the DynamoDB grammar most plausibly disagree on a corner case. Closing it raises confidence that the library is genuinely drop-in for expression-heavy workloads, not just for the common-path shapes the v1.0 floor proved.

The broader inventory in [[docs/notes/parity-coverage-gaps-in-operation-variants.md]] carries the rest — `ConsistentRead`, `ReturnConsumedCapacity`, legacy `ScanFilter` / `AttributesToGet`, multi-OR filter clauses, nested map filtering — which are real gaps but lower-risk than the parser surface.

## Next

Work the six in listed order. Each child note carries its own acceptance criteria. When the set closes, update [[docs/notes/parity-coverage-status.md]] Covered list and recompute the line/branch delta against the parity suite.
