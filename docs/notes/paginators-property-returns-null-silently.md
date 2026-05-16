# Paginators property returns null silently

Tags: todo, gap
DynamoDbClient.Paginators is auto-property never assigned — returns null for every call; client.Paginators.ScanAsync() throws NRE in consumer code.

## Observation

`src/DynamoDbLite/DynamoDbClient.cs:29` declares:

```csharp
public IDynamoDBv2PaginatorFactory? Paginators { get; }
```

The auto-property has no initializer and no assignment anywhere in the partial class — `get` returns `null` for every call on every instance. The property is mandated by `IAmazonDynamoDB`, so it cannot be removed.

A consumer writing the idiomatic AWS SDK pattern —

```csharp
await foreach (var item in client.Paginators.ScanAsync(req).Items)
{
    // ...
}
```

— gets a `NullReferenceException` at the `.ScanAsync(req)` access. No compile-time signal; the `?` annotation on the property is interface-mandated and looks normal.

Surfaced during the v1.0 public API surface audit; tracked as F1 in [`docs/public-review.md`](../public-review.md). The audit note is [v1-0-public-api-surface-audit](v1-0-public-api-surface-audit.md).

## Interpretation

Real `AmazonDynamoDBClient` instances always return a non-null factory. DynamoDbLite's `null` looks contract-conformant (the interface allows `?`) but breaks the actual consumer expectation — paginator-shaped code is the SDK's recommended way to iterate large Scan/Query results.

Silent `null` is the worst v1.0 outcome here: a method-shaped surface that compiles, accepts call-sites, then NREs in production. Worse than throwing because it pushes the failure away from the misuse and into whichever line happens to dereference first.

Three viable v1.0 landings:

1. **Implement** a minimal `IDynamoDBv2PaginatorFactory` that delegates `Query` / `Scan` pagination to the existing in-process loops. Largest splash; most consumer-friendly.
2. **Throw** `NotSupportedException` from a backing implementation with a clear "DynamoDbLite does not support `IDynamoDBv2PaginatorFactory` in v1.0; iterate manually with `LastEvaluatedKey`" message. Smallest splash; loud failure at the right site.
3. **Document** the `null` and ship as-is. Cheapest but worst UX.

Option 2 is the safest v1.0 default unless a consumer needs option 1 today.

## Next

- Decide which option ships in v1.0. Default to option 2 (throw with a clear message) unless a consumer of `Paginators` exists.
- If option 1, scaffold an `IDynamoDBv2PaginatorFactory` impl plus paginator types for `Scan` and `Query` minimally; defer the other surfaces (ListTables, etc.) until needed.
- If option 2 or 3, mention in the v1.0 release notes / README under a "Known limitations" subsection.
- Update [`docs/public-review.md`](../public-review.md) F1 Decision line once chosen.
