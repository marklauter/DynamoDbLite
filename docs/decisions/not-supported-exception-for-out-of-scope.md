---
title: NotSupportedException for out-of-scope operations
type: decision
summary: "Out-of-scope `IAmazonDynamoDB` methods throw `NotSupportedException`, not `NotImplementedException`: the contract member intentionally does not apply and never will."
tags: [scope, exceptions]
created: 2026-05-16
status: locked
supersedes: "[[out-of-scope-operations]]"
---

# `NotSupportedException` for out-of-scope operations

Status: Accepted (supersedes the exception-type choice in [Out-of-scope Operations](out-of-scope-operations.md))

The set of out-of-scope operations is unchanged.

## Decision

Out-of-scope `IAmazonDynamoDB` methods throw `NotSupportedException`, not `NotImplementedException`.

## Why

`NotImplementedException` signals "this code is incomplete, finish it later." `NotSupportedException` signals "this contract member intentionally does not apply to this implementation." The out-of-scope operations (backups, global tables, Kinesis streaming, PartiQL, contributor insights, resource policies) are the second case. They are meaningless for an in-process embedded emulator and will never be implemented. The exception type communicates that to callers and to anyone reading the source.

`ExecuteStatementAsync` and `ExecuteTransactionAsync` were already implemented this way. The rest of the unsupported surface now matches.

## Location

All stubs live in a single partial: `DynamoDbClient.Unsupported.cs`, organized by `#region` blocks matching the categories in [Out-of-scope Operations](out-of-scope-operations.md).
