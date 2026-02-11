# TagResourceAsync silently drops existing tags

- **Area:** DynamoDbClient.Admin / Tags
- **Priority:** High
- **Status:** Open

## Problem

TagResourceAsync validates merged tag count but only persists the new tags, discarding pre-existing tags. Calling `TagResourceAsync` with new tags overwrites all existing tags instead of merging. Existing tags not included in the request are silently lost. The merge logic was added for validation but the persistence call was never updated to write the merged result.

## Suggested Fix

Update `TagResourceAsync` to persist the merged tag set (existing + new) rather than just the new tags, matching AWS DynamoDB semantics where `TagResourceAsync` is additive. Only tags explicitly removed via `UntagResourceAsync` should disappear.

## Code References

- `src/DynamoDbLite/DynamoDbClient.Admin.cs` — TagResourceAsync method
