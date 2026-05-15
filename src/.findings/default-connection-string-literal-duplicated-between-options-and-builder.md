# Default connection string literal duplicated between options and builder

Severity: nit
Type: code
Location: `src/DynamoDbLite/DynamoDbLiteOptions.cs:4, src/DynamoDbLite/DynamoDbLiteOptionsBuilder.cs:7`
Principle: One source of truth
The default connection string literal appears verbatim in two files; changing the default requires editing both.


## Observation

`DynamoDbLiteOptions.cs:4`:
```csharp
public sealed record DynamoDbLiteOptions(
    string ConnectionString = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared");
```

`DynamoDbLiteOptionsBuilder.cs:7`:
```csharp
private string ConnectionString { get; set; } = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared";
```

Same literal, two files. No reference between them.

## Why it matters

If the default ever needs to change — and the [important finding on the default itself](default-connectionstring-shares-an-in-memory-cache-across-instances.md) argues it should — the change has to land in both places at once. Forgetting either leaves the two surfaces with different defaults: `new DynamoDbLiteOptions()` and `new DynamoDbLiteOptionsBuilder().Build()` would no longer agree. One source of truth, two copies, easy to drift.

## Suggested fix

Choose a single owner and reference it from the other:

```csharp
public sealed record DynamoDbLiteOptions(
    string ConnectionString = DefaultConnectionString)
{
    public const string DefaultConnectionString = "Data Source=DynamoDbLite;Mode=Memory;Cache=Shared";
}
```

Then the builder reads `DynamoDbLiteOptions.DefaultConnectionString`. Alternatively, the builder constructs `new DynamoDbLiteOptions().ConnectionString` for its initial value — also one source of truth, no public constant exposed.

If the [important finding](default-connectionstring-shares-an-in-memory-cache-across-instances.md) resolves option 1 (make ConnectionString required), this finding becomes moot — there's no default to duplicate.
