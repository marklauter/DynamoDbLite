# TagResourceAsync silently drops existing tags

- **Area:** DynamoDbClient.Admin / Tags
- **Type:** Bug
- **Priority:** High
- **Status:** Resolved

## Resolution

False positive. `SetTagsAsync` uses SQLite `INSERT ... ON CONFLICT DO UPDATE` (UPSERT), which is inherently additive — existing tags not in the request are untouched. A dedicated test (`TagResource_Preserves_Existing_Tags_When_Adding_New`) was added to confirm this behaviour.
