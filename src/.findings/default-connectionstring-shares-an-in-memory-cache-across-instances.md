# Default ConnectionString shares an in-memory cache across instances

Severity: important
Type: code
Location: `src/DynamoDbLite/DynamoDbLiteOptions.cs:4, src/DynamoDbLite/DynamoDbLiteOptionsBuilder.cs:7, src/DynamoDbLite/DynamoDbClient.cs:12`
Principle: The easy path is the correct path
DynamoDbLiteOptions defaults its ConnectionString to a single named in-memory shared cache; two default-constructed clients in the same process silently share a database.


## Observation

`DynamoDbLiteOptions` is a public record with one positional parameter whose default value is a fixed connection string:

```csharp
public sealed record DynamoDbLiteOptions(
    string ConnectionString = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared");
```

`DynamoDbLiteOptionsBuilder` carries the same literal as its initial value (`DynamoDbLiteOptionsBuilder.cs:7`). The `DynamoDbClient` primary constructor in turn defaults its `options` parameter to `null` and instantiates `new DynamoDbLiteOptions()` when nothing is supplied (`DynamoDbClient.cs:12-22`).

`Mode=Memory;Cache=Shared` with a fixed `Data Source` name shares one in-memory SQLite database across every connection to that name in the process. Two default-constructed `DynamoDbClient`s in the same process therefore share state — different tables, but the same physical database file in memory. The shared-cache semantics are documented SQLite behavior; the issue is that the *default* values invite it.

Concrete evidence: the test suite hit this exact collision on `ServiceCollectionExtensionsTests` (two tests using the default cache name) until a separate change isolated them. The defensive `EnsureSkNumColumn` shipped in commit `6c6a8ca` and then removed in `9a26b92` was patching this same symptom from the test side.

## Why it matters

The path of least resistance — `new DynamoDbClient()`, or `new DynamoDbLiteOptions()`, or `AddDynamoDbLite(_ => { })` — produces a client that silently shares a database with every other default-constructed client in the same process. Real consumers won't always notice; they'll notice only when symptoms surface (cross-test pollution, unexpected items, weird DDL conflicts). The Norman / "easy path" principle says the dangerous thing should take real effort, not be the default.

Once v1.0 ships the default value, changing it is a breaking change for any consumer that was relying on the current behavior (legitimately or accidentally). Now is the cheapest moment to tighten this.

## Suggested fix

Three viable directions, listed in order of strictness:

1. Make `ConnectionString` required — remove the default value from `DynamoDbLiteOptions` and require the parameter on both `new DynamoDbLiteOptions(...)` and `new DynamoDbClient(options, ...)`. Forces an explicit decision; eliminates the foot-gun entirely. Breaks `new DynamoDbClient()` and `AddDynamoDbLite(_ => { })` as currently written, but in v1.0 these forms are arguably wrong anyway.
2. Default to a unique cache per instance — `Data Source=DynamoDbLite_{Guid.NewGuid():N};Mode=Memory;Cache=Shared`. Keeps the "just works" ergonomics; eliminates accidental sharing. Slight surprise: two default instances *aren't* the same database, which some test code today might actually rely on.
3. Keep the default but document the sharing semantics loudly — `<remarks>` on the record explaining that defaults share state, plus a banner in the README. Cheapest; doesn't fix the foot-gun, just signs it.

Recommended: (1) for v1.0. The library's job is to give consumers a fast in-process emulator; the cache-shared-across-instances behavior is a side effect of the choice of default value, not a deliberate feature. Forcing the connection string at construction time makes the configuration decision explicit and aligns with the writing-csharp "make invalid states unrepresentable" stance.
