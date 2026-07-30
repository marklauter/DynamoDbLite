---
title: Parity test — scan pagination
summary: Cross-backend assertion that Scan's ExclusiveStartKey/LastEvaluatedKey round-trip matches real DynamoDB; Query pagination is covered, Scan is not.
tags: [note, todo, parity, v1.1]
created: 2026-05-27
---

# Parity test — scan pagination

Cross-backend assertion that `Scan`'s `ExclusiveStartKey` / `LastEvaluatedKey` round-trip matches real DynamoDB; Query pagination is covered, Scan is not.

## What's missing

`QueryParityTests` exercises `Limit` + `LastEvaluatedKey` round-trip for `Query`. No parity test does the equivalent for `Scan` even though `ScanParityTests` covers `FilterExpression`, parallel-scan segments, `IN`, and `size()`.

Specific assertions absent:

- `Scan` with `Limit = N` returns at most `N` items and a non-null `LastEvaluatedKey` when more rows exist.
- Replaying with `ExclusiveStartKey = LastEvaluatedKey` resumes exactly where the prior page ended, with no duplicates or skips.
- The terminal page returns `LastEvaluatedKey = null`.
- `Scan` with `FilterExpression` + `Limit` — `Limit` bounds the pre-filter scan window, so a filtered scan can return fewer than `Limit` items while still setting `LastEvaluatedKey` (this is a common drop-in confusion).

## Why parser-divergence risk

Pagination cursors are serialized as `Dictionary<string, AttributeValue>` and must round-trip byte-stable enough that a cursor produced by DdbLite resumes correctly when fed back to DdbLite, and that the resumption order matches real DynamoDB's scan order. Silent drift here is plausible: tests pass on the no-cursor path, the cursor path looks reasonable, only users paginating large scans see wrong totals or duplicates.

## Acceptance

Add cases to `ScanParityTests.cs`:

- Plain pagination: seed 25 items, scan with `Limit = 10`, walk three pages, assert union of pages equals seed set with no duplicates.
- Filtered pagination: seed 25 items where 10 match a filter, scan with `Limit = 5` + `FilterExpression`, walk pages until `LastEvaluatedKey` is null, assert the filtered subset emerges correctly across page boundaries.
- Parameterize over the three backends per the [[docs/notes/parity-coverage-status.md]] strategy.

## Sequencing

Third in the [[docs/notes/parity-parser-divergence-test-set.md]] epic. Lower parser risk than expression breadth but high consumer-surface impact — pagination drift is the kind of bug a drop-in user files first.

This gap was not in [[docs/notes/parity-coverage-gaps-in-operation-variants.md]]; surfaced by the 2026-05-27 audit.
