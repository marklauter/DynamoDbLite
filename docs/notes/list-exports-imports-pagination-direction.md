# ListExports / ListImports pagination filter direction

Tags: known-limitation, pagination, exports, imports
The `NextToken` continuation in `ListExportRecordsAsync` and `ListImportRecordsAsync` filters with the wrong ROWID comparison direction; second-page results overlap with page 1 instead of returning older records.

## Observation

`SqliteStore.cs` (≈ lines 1490 and 1592) builds the listing query as:

```sql
SELECT ...
FROM exports                -- or imports
WHERE table_name = @tableName
  AND ROWID > (SELECT ROWID FROM exports WHERE export_arn = @nextToken)
ORDER BY start_time DESC
LIMIT @maxResults;
```

`NextToken` is set to the **last row of the previous page** (`Export.cs:144`, `Import.cs:193`) — i.e. the oldest row on a `start_time DESC` page. The filter then says "rows whose ROWID is greater than that oldest token's ROWID" — but on `start_time DESC`, the newer rows already returned on page 1 also satisfy `ROWID > tokenRowId`. Result: page 2 returns a subset of page 1 (excluding only the very oldest token-row), missing the actually-newer pages of history.

Concrete example with three exports A, B, C (ROWIDs 1, 2, 3 in insertion order, monotonic in `start_time`):
- Page 1: `ORDER BY start_time DESC LIMIT 2` → `[C, B]`. `NextToken = B.ExportArn` (ROWID 2).
- Page 2: `WHERE ROWID > 2 ORDER BY start_time DESC` → `[C]` — already returned on page 1, and A is never visible.

## Interpretation

The intended pagination over a `DESC`-ordered result set should advance to **older** records, i.e. either `ROWID <` against the token's ROWID, or `start_time <` against the token's start_time.

No test exercises continuation correctness today — `ExportTests.cs` and `ImportTests.cs` only have a `ListExports_With_NextToken_Accepts_Continuation` test that asserts the response is non-null. Coverage walks the line but does not exercise the contract.

## Proposed fix

Two options, both touch only `SqliteStore.cs`:

1. **Flip the comparison.** Change `ROWID >` to `ROWID <` in both `ListExportRecordsAsync` and `ListImportRecordsAsync`. Works because `start_time` is monotonic in insertion order in this codebase (both call `DateTime.UtcNow.ToString("O")` at insert).
2. **Use `start_time` directly.** Filter by `start_time < (SELECT start_time FROM exports WHERE export_arn = @nextToken)`. More robust to insertion-order anomalies but adds a sub-select per call.

Option 1 is the minimal fix. Both options preserve the public token contract (caller still passes back the opaque `NextToken` they received).

## Next

- Promote the `_Accepts_Continuation` tests in `ExportTests.cs` / `ImportTests.cs` to assert disjoint page-1/page-2 results that union to the full set (the strong assertion drafted but weakened during C1 coverage work).
- Land the fix in a separate change.
