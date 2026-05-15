# DynamoDbLiteOptionsBuilder.Build() is internal

Severity: nit
Type: code
Location: `src/DynamoDbLite/DynamoDbLiteOptionsBuilder.cs:15`
Principle: Inference, not annotation
Build() returns the configured DynamoDbLiteOptions but is internal; non-DI consumers cannot use the fluent builder to construct a validated options instance.


## Observation

```csharp
public sealed class DynamoDbLiteOptionsBuilder
{
    public DynamoDbLiteOptionsBuilder WithConnectionString(string connectionString) { ... }

    internal DynamoDbLiteOptions Build() => new(ConnectionString);
}
```

`WithConnectionString` is public and performs validation that throws `DynamoDbLiteConfigurationException` on malformed input. `Build()` is `internal`, only callable from `ServiceCollectionExtensions.AddDynamoDbLite`.

Consumers who construct `DynamoDbClient` directly (without DI) have two choices:
- `new DynamoDbLiteOptions("Data Source=...")` — bypasses the builder's validation entirely; an invalid string only fails later when SQLite tries to open the connection.
- Use `AddDynamoDbLite` through a `ServiceCollection`, even when DI is overkill for their context.

## Why it matters

The public surface is asymmetric: validation is gated behind the builder, the builder is gated behind DI. A console-app or test-fixture consumer who wants the validation cannot get to it without instantiating a `ServiceCollection`. The principle is small ("inference, not annotation" / minimal-friction API) but compounds — the friction nudges consumers toward bypassing the builder, which means the validation it adds is unevenly applied.

This is `nit` because there's a viable alternative (catch the SqliteException at first use), but for v1.0 it's worth a decision.

## Suggested fix

Two options:

1. Make `Build()` public. The builder becomes usable standalone: `var options = new DynamoDbLiteOptionsBuilder().WithConnectionString("...").Build();`. The DI extension keeps working unchanged.
2. Expose `DynamoDbLiteOptionsBuilder.Validate` as a static helper (`public static string ValidateConnectionString(string)`), keep `Build()` internal. Less ceremony for the common case of "I just want to validate a string."

(1) matches the existing builder shape and adds one keyword. The DI extension's call site stays identical.
