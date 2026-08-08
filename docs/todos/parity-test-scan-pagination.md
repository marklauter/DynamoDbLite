---
title: Parity test — scan pagination
type: todo
summary: "Scan had no cross-backend assertion for the ExclusiveStartKey/LastEvaluatedKey round-trip while Query did. Closed: five paginated-scan cases now run against all three backends, covering the table and index code paths, with cursor contents pinned rather than cursor presence."
tags: [parity, v1.1]
created: 2026-05-27
priority: medium
status: done
part-of: "[[parity-parser-divergence-test-set]]"
cites: "[[parity-coverage-status]]"
---

# Parity test — scan pagination

Closed. `ScanParityTests` gained five cases, each parameterized over DdbLite, DdbLiteFile, and DynamoDbLocal:

- `Scan_with_Limit_paginates_via_LastEvaluatedKey_without_duplicates_or_gaps` — 25 items at `Limit = 10`, walked to exhaustion. Asserts exactly three pages. Each non-terminal page holds exactly `Limit` items, with `Count` and `ScannedCount` both equal to `Limit` because no filter runs. The terminal page holds the remaining five, and the pages concatenated and sorted equal the sorted seed set. Nothing asserts the terminal page lacks a cursor, because `ScanAllPagesAsync` stops only on an empty one: a backend that never cleared it overruns the page count instead.
- `Scan_with_Limit_dividing_the_table_exactly_ends_on_an_empty_page` — 20 items at `Limit = 10`. A page that fills the `Limit` carries a cursor even when it drained the table, so the walk ends on an empty third page rather than on the second full one. That trailing page reports `Count` and `ScannedCount` of zero. All three backends agree.
- `Scan_with_FilterExpression_and_Limit_bounds_the_pre_filter_window` — 25 items of which 10 match, at `Limit = 5`. 25 divides by 5, so this case ends on an empty page too. Asserts `Count` equals the returned item count on every page, each cursor-carrying page scanned exactly `Limit` rows whatever survived the filter, the trailing page reports zero for both counts, all 10 matching items arrive across the pages, and at least one cursor-carrying page returns fewer than `Limit` items.

- `Scan_on_a_global_secondary_index_paginates_via_LastEvaluatedKey` and `Scan_on_a_local_secondary_index_paginates_via_LastEvaluatedKey` — 25 items at `Limit = 10` over each index kind. `ScanIndexAsync` extracts and builds cursors on its own code path, and it had no coverage. A GSI cursor carries `GsiPK`, `GsiSK`, `PK`, and `SK`; an LSI cursor carries `PK`, `SK`, and `LsiSK`. All three backends agree on both shapes.

Every cursor is checked for contents. `AssertCursorShape` asserts it holds exactly the `PK` attribute and nothing else; `AssertCursorNamesLastItem` adds that the value is the page's last returned item. Only the shape check applies under a `FilterExpression`, where the cursor names the last row scanned rather than the last row returned. A cursor carrying extra attributes still round-trips inside one backend, so nothing else in the suite catches that.

Scan order is unspecified, so pages are compared as sorted sequences rather than in arrival order. The short-page assertion in the third case survives any order too, given the pre-filter semantics it asserts alongside: the cursor-carrying pages span more than two windows while holding at most 10 matches between them, so one of them returns fewer than 5 items. A backend applying `Limit` after the filter fills every page instead and fails there.

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
