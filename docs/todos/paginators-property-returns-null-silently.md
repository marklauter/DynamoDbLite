---
title: Paginators property returns null silently
type: todo
summary: "DynamoDbClient.Paginators is an auto-property never assigned, so it returns null and NREs in consumer code."
tags: [gap]
created: 2026-05-16
priority: high
status: open
constrained-by: "[[not-supported-exception-for-out-of-scope]]"
---

# Paginators property returns null silently

DynamoDbClient.Paginators is an auto-property that is never assigned. It returns null for every call, so client.Paginators.ScanAsync() throws an NRE in consumer code.

## Observation

`src/DynamoDbLite/DynamoDbClient.cs:29` declares:

```csharp
public IDynamoDBv2PaginatorFactory? Paginators { get; }
```

The auto-property has no initializer and no assignment anywhere in the partial class. `get` returns `null` for every call on every instance. The property is mandated by `IAmazonDynamoDB`, so it cannot be removed.

A consumer writing the idiomatic AWS SDK pattern:

```csharp
await foreach (var item in client.Paginators.ScanAsync(req).Items)
{
    // ...
}
```

gets a `NullReferenceException` at the `.ScanAsync(req)` access. There is no compile-time signal. The `?` annotation on the property is interface-mandated and looks normal.

Surfaced during the v1.0 public API surface audit; tracked as F1 in [`docs/public-review.md`](../public-review.md). The audit note is [v1-0-public-api-surface-audit](v1-0-public-api-surface-audit.md).

## Interpretation

Real `AmazonDynamoDBClient` instances always return a non-null factory. DynamoDbLite's `null` is contract-conformant, since the interface allows `?`, but it breaks consumer expectation. Paginator-shaped code is the SDK's recommended way to iterate large Scan/Query results.

Silent `null` is the worst v1.0 outcome here: a surface that compiles, accepts call-sites, then NREs in production. It is worse than throwing because it pushes the failure away from the misuse and into whichever line happens to dereference first.

Three options for v1.0:

1. **Implement** a minimal `IDynamoDBv2PaginatorFactory` that delegates `Query` / `Scan` pagination to the existing in-process loops. Most work; nothing for consumers to change.
2. **Throw** `NotSupportedException` from a backing implementation with a clear "DynamoDbLite does not support `IDynamoDBv2PaginatorFactory` in v1.0; iterate manually with `LastEvaluatedKey`" message. Least work; the failure lands at the call site.
3. **Document** the `null` and ship as-is. Cheapest but worst UX.

Option 2 is the safest v1.0 default unless a consumer needs option 1 today.

## Next

- Decide which option ships in v1.0. Default to option 2 (throw with a clear message) unless a consumer of `Paginators` exists.
- If option 1, scaffold a minimal `IDynamoDBv2PaginatorFactory` impl plus paginator types for `Scan` and `Query`; defer the other surfaces (ListTables, etc.) until needed.
- If option 2 or 3, mention in the v1.0 release notes / README under a "Known limitations" subsection.
- Update [`docs/public-review.md`](../public-review.md) F1 Decision line once chosen.
