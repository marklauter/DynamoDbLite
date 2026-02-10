# ExtractTransactWriteAction returns partially-populated record

- **Area:** DynamoDbClient.Transactions
- **Priority:** Low
- **Status:** Open

## Problem

`ExtractTransactWriteAction` returns a `ResolvedTransactWriteAction` with `default!` for `Pk`, `Sk`, `KeyInfo`, and `Index` — fields that are only meaningful after the caller computes them. This is confusing because the record type implies all fields are valid, but only a subset is populated by the extraction step.

## Suggested Fix

Introduce a lightweight intermediate type (e.g., `ExtractedTransactAction` positional record) that holds only the fields populated during extraction: `TableName`, `Key`, `Item`, `ConditionExpression`, expression attribute maps, `UpdateExpression`, `ReturnValuesOnConditionCheckFailure`, and `ActionType`. The caller then constructs the full `ResolvedTransactWriteAction` from this intermediate plus the computed `Pk`/`Sk`/`KeyInfo`/`Index`.

## Code References

- `src/DynamoDbLite/DynamoDbClient.Transactions.cs:310` — `ExtractTransactWriteAction` method
- `src/DynamoDbLite/DynamoDbClient.Transactions.cs:79` — caller rebuilds the record with computed fields

## Notes

Cosmetic issue only — no correctness impact. The `default!` fields are never read from the intermediate value.
