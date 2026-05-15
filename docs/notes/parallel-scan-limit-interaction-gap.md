# Parallel scan + Limit interaction gap

Tags: release, v1.1, known-limitation, parallel-scan
ScanRequest with both TotalSegments and Limit applies Limit before the segment filter; real DynamoDB applies Limit within the segment.


## Observation

The parallel-scan fix in commit `0e435bd` post-filters rows by segment hash AFTER the SQLite store applies `Limit`. With `Limit=10` and `TotalSegments=2`, the store returns up to 10 rows; the segment filter then keeps only the ~half hashing to the requested segment. Result: a single segment can return fewer items than `Limit` even when more items in that segment exist beyond what was sampled.

Real DynamoDB partitions items by hash *before* applying `Limit`, so `Limit` bounds the result count within the segment, not the pre-partition pool.

No parity test exercises both knobs simultaneously — the gap is unmonitored.

## Interpretation

The fix shipped because the parallel-scan parity test (no Limit) needed it and the contract for the no-Limit case is correct. For consumers who use `Limit` + `TotalSegments` together, behavior diverges from real DynamoDB silently. Visibility is poor: tests pass, code looks reasonable, only users with the specific combination see the wrong count.

A correct implementation either:
1. Filters by segment before paging (push the hash into the SQL `WHERE` clause), then applies `Limit`.
2. Iterates the store result, filtering by segment, stops at `Limit` matches.

Option 2 is simpler in C#; option 1 keeps the work in SQLite.

## Next

- Add a parity test: scan a large table with `TotalSegments=2`, `Limit=5` on each segment. Assert each segment returns `Limit` items when more than `Limit` items hash to it, and that `LastEvaluatedKey` is set correctly.
- Implement the fix in `DynamoDbClient.Query.cs` (`ScanAsync` and `ScanIndexAsync`).
- Document under `docs/parity.md` "Known limitations" until fixed; remove from CHANGELOG known-limitations when closed.

Listed under [v1-0-changelog-and-release-notes](v1-0-changelog-and-release-notes.md) "Known limitations" in the meantime.
