---
title: ListExports / ListImports pagination filter direction
type: note
summary: "The NextToken continuation compared ROWID against a start_time DESC sort, so page 2 overlapped page 1. Fixed by comparing start_time; records sharing a timestamp can still be skipped."
tags: [known-limitation, pagination, exports, imports]
created: 2026-05-16
status: evolving
constrained-by: "[[behavioral-fidelity]]"
---

# ListExports / ListImports pagination filter direction

The `NextToken` continuation in `ListExportRecordsAsync` and `ListImportRecordsAsync` compared `ROWID` in the wrong direction against a `start_time DESC` sort, so pages after the first re-read rows already returned and skipped older ones. That is fixed. A narrower limitation remains: records sharing a `start_time` can be skipped.

## What was wrong

The listing query resumed with `ROWID > (SELECT ROWID FROM exports WHERE export_arn = @nextToken)` while ordering `start_time DESC`. `NextToken` is the last row of the previous page, which on a descending sort is its oldest row. Filtering to rows above that ROWID re-selects the newer rows already returned and never reaches the older ones.

With five exports at `MaxResults = 2`, enumeration yielded three records, one of them twice, while the unpaged call returned all five.

## What changed

The cursor now compares the column the query sorts on:

```sql
start_time < (SELECT start_time FROM exports WHERE export_arn = @nextToken)
```

Same shape in `ListImportRecordsAsync` against `imports`. The public token contract is unchanged — the caller still passes back the opaque `NextToken` they received.

Comparing `ROWID <` instead was measured and rejected. Within a tie group SQLite emits rows in scan order, which ascends against the descending sort, so a ROWID cursor skips in the same positions and duplicates in the rest. Uniqueness of the cursor column buys nothing when the sort is on a different column.

## What remains

`start_time` is not unique and the comparison is strictly less-than, so when two records share a timestamp one of them can be skipped. No single-column cursor closes this. A composite `(start_time, arn)` token would, and it was not built: whether these operations should exist at all is an open question, since the AWS contract they implement is defined against S3 and this implementation substitutes the local filesystem.

Exposure is low. `start_time` carries tick resolution from `DateTime.UtcNow.ToString("O")`, and each record is written with file I/O in between, so distinct timestamps are the normal case.

## Coverage

`ListExports_CallerSuppliedNextToken_ResumesAfterIt` and its `ListImports` twin assert that a resumed enumeration delivers exactly the records the first page did not, each once. Restoring the old `ROWID >` cursor turns 16 tests red, so the defect is pinned against regression.

The `_Accepts_Continuation` tests in `ExportTests.cs` and `ImportTests.cs` still assert only that the response is non-null. They are now weaker than the behavior the code delivers, and are candidates for strengthening.
