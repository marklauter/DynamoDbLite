---
title: Out-of-scope Operations
type: decision
summary: "Backups, global tables, Kinesis streaming, PartiQL, contributor insights, resource policies, and vector search are permanently out of scope. Superseded on the exception type only; the set of operations still stands."
tags: [scope, api-surface]
created: 2026-05-16
status: superseded
superseded-by: "[[not-supported-exception-for-out-of-scope]]"
---

# Out-of-scope Operations

Superseded on the exception type only by [`NotSupportedException` for out-of-scope operations](not-supported-exception-for-out-of-scope.md). The set of out-of-scope operations below still stands.

These operations are not meaningful for a local embedded emulator and will remain as `NotImplementedException` stubs:

- **Backup & restore:** `CreateBackup`, `DeleteBackup`, `RestoreTableFromBackup`, PITR
- **Global tables & replication:** `CreateGlobalTable`, replica management
- **Kinesis streaming:** `EnableKinesisStreamingDestination` and related
- **PartiQL:** `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`
- **Contributor insights / resource policies**
- **Vector search:** `SearchVectors`

## Amendment, 2026-08-10

`SearchVectors` was added to `IAmazonDynamoDB` in AWSSDK.DynamoDBv2 4.0.103, after this note was
written, and joins the set for the same reason as the rest. Vector similarity search needs an
approximate-nearest-neighbor index and configurable distance functions (`COSINE`, `EUCLIDEAN`,
`DOT_PRODUCT`). SQLite has no analog, and building one is a different project.

Adding a member to `IAmazonDynamoDB` is source-compatible for callers of the SDK and breaking for
implementors of it. DynamoDbLite is an implementor, so an SDK release whose version suggests a patch
can stop the build outright. Expect this on future SDK bumps: the fix is a stub in
`DynamoDbClient.Unsupported.cs` when the operation is out of scope, and a scope decision when it
is not.
