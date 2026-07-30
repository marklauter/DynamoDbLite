---
title: Out-of-scope Operations
type: decision
summary: "Backups, global tables, Kinesis streaming, PartiQL, contributor insights, and resource policies are permanently out of scope. Superseded on the exception type only; the set of operations still stands."
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
