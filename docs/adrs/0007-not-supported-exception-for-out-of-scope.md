# ADR 0007 — `NotSupportedException` for out-of-scope operations

Status: Accepted

Supersedes the exception-type choice in [ADR 0006](0006-out-of-scope-operations.md). The set of out-of-scope operations is unchanged.

## Decision

Out-of-scope `IAmazonDynamoDB` methods throw `NotSupportedException`, not `NotImplementedException`.

## Why

`NotImplementedException` signals "this code is incomplete — finish it later." `NotSupportedException` signals "this contract member intentionally does not apply to this implementation." The operations listed in ADR 0006 (backups, global tables, Kinesis streaming, PartiQL, contributor insights, resource policies) are the second case: they are meaningless for an in-process embedded emulator and will never be implemented. The exception type should communicate that to callers and to anyone reading the source.

This also aligns with how `ExecuteStatementAsync` and `ExecuteTransactionAsync` were already implemented — the rest of the unsupported surface now matches.

## Location

All stubs live in a single partial: `DynamoDbClient.Unsupported.cs`, organized by `#region` blocks matching the categories in ADR 0006.
