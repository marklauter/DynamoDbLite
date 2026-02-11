# Harden ApplyAttributeUpdates legacy method

- **Area:** DynamoDbClient.Crud
- **Priority:** Medium
- **Status:** Open

## Problem

The legacy `ApplyAttributeUpdates` method lacks key attribute validation and has incomplete set semantics. Specifically:
1. Key attributes (PK/SK) can be silently overwritten — real DynamoDB rejects this
2. ADD for SS/NS sets uses `AddRange` without deduplication — real DynamoDB enforces set uniqueness
3. DELETE and ADD for BS (binary sets) are not handled
4. DELETE for BS falls through to the default case which overwrites the attribute with `update.Value`

## Suggested Fix

1. Validate that key attributes are not modified — throw `AmazonDynamoDBException` matching the real error message
2. Use `HashSet`-based deduplication when applying ADD to SS/NS sets
3. Handle DELETE and ADD for BS (binary sets) analogous to SS/NS
4. Consider extracting shared logic between `ApplyAttributeUpdates` and `UpdateExpressionEvaluator` for ADD/DELETE set operations

## Code References

- `src/DynamoDbLite/DynamoDbClient.Crud.cs:ApplyAttributeUpdates` — legacy attribute update method used by DynamoDBContext's Document Model layer

## Notes

DynamoDBContext's Save uses PUT action (not ADD/DELETE for sets), so the gaps mainly affect direct legacy API usage. Key validation gap is the most important fix.
