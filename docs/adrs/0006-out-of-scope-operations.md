# ADR 0006 — Out-of-scope Operations

Status: Superseded by [ADR 0007](0007-not-supported-exception-for-out-of-scope.md) — the set of out-of-scope operations below still stands; only the exception type changed (now `NotSupportedException`).

These operations are not meaningful for a local embedded emulator and will remain as `NotImplementedException` stubs:

- **Backup & restore:** `CreateBackup`, `DeleteBackup`, `RestoreTableFromBackup`, PITR
- **Global tables & replication:** `CreateGlobalTable`, replica management
- **Kinesis streaming:** `EnableKinesisStreamingDestination` and related
- **PartiQL:** `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`
- **Contributor insights / resource policies**
