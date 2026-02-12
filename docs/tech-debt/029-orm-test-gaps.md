# DynamoDBContext ORM test coverage gaps

- **Area:** DynamoDbLite.Tests / DynamoDbContext
- **Priority:** Medium
- **Status:** Open

## Problem

The DynamoDBContext ORM tests have coverage gaps in several areas that could hide behavioral differences from real DynamoDB:

1. **Pagination edge cases:** No tests for GetNextSetAsync with 0 items, exactly 1 item, or calling after exhaustion. Current pagination tests use 5+ items with loose assertions (`Count > 0 && Count <= 5`).
2. **ORM transactions:** DynamoDBContext exposes `CreateTransactGet<T>()` and `CreateTransactWrite<T>()` (plus multi-table variants), but no ORM-level transaction tests exist. Low-level transactions are tested separately.
3. **Type boundary values:** No tests for int.MinValue/MaxValue, negative numbers, zero, or double edge cases (0.0, very small values).
4. **Key edge cases:** No tests for special characters in keys (unicode, spaces, `#`, `/`), very long keys, or items with only key attributes (no other properties).
5. **Collection edge cases:** No tests for single-element collections, null collection properties, or large collections (1000+ elements).
6. **Sparse GSI:** No test for items without GSI key attributes (sparse index behavior through ORM).

## Suggested Fix

Add tests incrementally, prioritizing ORM transactions and pagination edge cases first since those exercise the most distinct code paths. Type and key edge cases can follow.

## Code References

- `src/DynamoDbLite.Tests/DynamoDbContext/DynamoDbContextTests.cs` — main test file
- `src/DynamoDbLite.Tests/Models/` — model classes used by tests
