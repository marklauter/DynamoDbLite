---
title: Parity test — scan pagination
type: todo
summary: "Scan had no cross-backend assertion for the ExclusiveStartKey/LastEvaluatedKey round-trip while Query did. Closed: two paginated-scan cases now run against all three backends."
tags: [parity, v1.1]
created: 2026-05-27
priority: medium
status: done
part-of: "[[parity-parser-divergence-test-set]]"
cites: "[[parity-coverage-status]]"
---

# Parity test — scan pagination

Closed. `ScanParityTests` gained two cases, both parameterized over DdbLite, DdbLiteFile, and DynamoDbLocal:

- `Scan_with_Limit_paginates_via_LastEvaluatedKey_without_duplicates_or_gaps` — 25 items at `Limit = 10`, walked to exhaustion. Asserts every page holds at most `Limit` items and scans at most `Limit`, the first page carries a cursor, the terminal page does not, and the pages concatenated and sorted equal the sorted seed set.
- `Scan_with_FilterExpression_and_Limit_bounds_the_pre_filter_window` — 25 items of which 10 match, at `Limit = 5`. Asserts `ScannedCount` stays within `Limit`, the 10 matching items all arrive across the pages, and at least one cursor-carrying page returns fewer than `Limit` items.

Scan order is unspecified, so pages are compared as sorted sequences rather than in arrival order. The second case's short-page assertion also survives any order. At least four pages carry a cursor, and those pages hold at most 10 matches between them, so one of them returns fewer than 5 items.

The record below is what the gap was.

## What was missing

`QueryParityTests` exercises `Limit` + `LastEvaluatedKey` round-trip for `Query`. No parity test does the equivalent for `Scan` even though `ScanParityTests` covers `FilterExpression`, parallel-scan segments, `IN`, and `size()`.

Specific assertions absent:

- `Scan` with `Limit = N` returns at most `N` items and a non-null `LastEvaluatedKey` when more rows exist.
- Replaying with `ExclusiveStartKey = LastEvaluatedKey` resumes exactly where the prior page ended, with no duplicates or skips.
- The terminal page returns `LastEvaluatedKey = null`.
- `Scan` with `FilterExpression` + `Limit` — `Limit` bounds the pre-filter scan window, so a filtered scan can return fewer than `Limit` items while still setting `LastEvaluatedKey` (this is a common drop-in confusion).

## Why parser-divergence risk

Pagination cursors are serialized as `Dictionary<string, AttributeValue>`. They must round-trip byte-stable enough that a cursor produced by DdbLite resumes correctly when fed back to DdbLite, and that the resumption order matches real DynamoDB's scan order. Drift here stays silent: tests pass on the no-cursor path, the cursor path looks reasonable, and only users paginating large scans see wrong totals or duplicates.

## Acceptance

Add cases to `ScanParityTests.cs`:

- Plain pagination: seed 25 items, scan with `Limit = 10`, walk three pages, assert union of pages equals seed set with no duplicates.
- Filtered pagination: seed 25 items where 10 match a filter, scan with `Limit = 5` + `FilterExpression`, walk pages until `LastEvaluatedKey` is null, assert the filtered subset emerges correctly across page boundaries.
- Parameterize over the three backends per the [[parity-coverage-status]] strategy.

## Sequencing

Third in the [[parity-parser-divergence-test-set]] epic. Lower parser risk than expression breadth, higher consumer-surface impact. Pagination drift is the kind of bug a drop-in user files first.

This gap was not in [[parity-coverage-gaps-in-operation-variants]]; surfaced by the 2026-05-27 audit.
