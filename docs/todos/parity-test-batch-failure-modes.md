---
title: Parity test — batch failure modes
type: todo
summary: "Cross-backend assertion for BatchWriteItem condition-check rejection shape and oversize-batch rejection (>25 items on BatchWriteItem, >100 on BatchGetItem); UnprocessedItems throttling is excluded."
tags: [parity, v1.1]
created: 2026-05-27
priority: medium
status: open
part-of: "[[parity-parser-divergence-test-set]]"
refines: "[[parity-coverage-gaps-in-operation-variants]]"
---

# Parity test — batch failure modes

Cross-backend assertion for `BatchWriteItem` condition-check rejection shape and oversize-batch rejection (>25 items on `BatchWriteItem`, >100 on `BatchGetItem`); `UnprocessedItems` throttling is excluded.

## What's missing

`BatchParityTests` covers `BatchGetItem` happy path and `BatchWriteItem` put + delete across one and two tables. Missing failure-mode coverage that the parity framework can exercise:

- Real DynamoDB does **not** support `ConditionExpression` on `BatchWriteItem`. Asserting that all three backends reject the same way (the AWS SDK should refuse to construct it, or the server rejects) closes a known divergence vector.
- Oversize `BatchWriteItem`: >25 items in one call → `ValidationException`.
- Oversize `BatchGetItem`: >100 keys in one call → `ValidationException`.

`UnprocessedItems` throttling and partial-failure semantics are excluded — dynamodb-local does not throttle, so the parity framework cannot force the path. Document the exclusion in the test class comment.

## Why parser-divergence risk

The cap-rejection paths are validation surfaces. The library could silently accept an oversize batch and process it as a stream of single calls, which is a drop-in divergence. The in-proc main suite would not catch it because no consumer error is observable.

## Acceptance

Add to `BatchParityTests.cs`:

- Oversize `BatchWriteItem` (26 items): assert `ValidationException` (or library-equivalent rejection) across the three backends.
- Oversize `BatchGetItem` (101 keys): assert the same.
- Class-level comment noting that `UnprocessedItems` partial-failure tests are intentionally absent because dynamodb-local does not throttle; main suite covers DdbLite/DdbLiteFile behavior in isolation.

## Sequencing

Sixth and last in the [[docs/notes/parity-parser-divergence-test-set.md]] epic. The surface is narrow and validation-only, and closing it supports drop-in confidence. Subset of the broader Batch gap section in [[docs/notes/parity-coverage-gaps-in-operation-variants.md]] (which also covers `UnprocessedItems`; explicitly excluded here for parity-framework feasibility).
