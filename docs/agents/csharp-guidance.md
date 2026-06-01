---
title: C# Guidance
summary: Build, test, and format commands plus the C# conventions for DynamoDbLite that the analyzers and .editorconfig do not catch.
---

# C# Guidance

## Commands

- Build: `dotnet build "DynamoDbLite.slnx"`
- Test: `dotnet test "DynamoDbLite.slnx"`
- Single test: `dotnet test "DynamoDbLite.slnx" --filter "FullyQualifiedName~MethodName"` (xunit v3 — `FullyQualifiedName`, not `FullyQualifiedClassName`; `~` matches a substring of the namespace.class.method name, so a class name works too)
- Format (fix): `dotnet format "DynamoDbLite.slnx" --severity info`
- Format (verify, CI-style): `dotnet format "DynamoDbLite.slnx" --severity info --verify-no-changes`

`.editorconfig` and `AnalysisLevel=latest-all` enforce most style rules — run the format command above and fix everything before committing. The conventions below are the ones tooling does *not* catch.

## Performance posture

Hot-path DB-emulation layer — default to allocation-aware patterns; sacrifice readability only when measured to matter.

## Records

Positional records, not property-bodied:

```csharp
public sealed record DynamoDbLiteOptions(string ConnectionString);
```

For records with 2+ parameters, one param per line, 4-space indent, blank line between records:

```csharp
internal sealed record ComparisonNode(
    Operand Left,
    string Operator,
    Operand Right)
    : ConditionNode;

internal sealed record BetweenNode(
    Operand Value,
    Operand Lower,
    Operand Upper)
    : ConditionNode;
```

`readonly record struct` for value types ≤16 bytes. Seal everything by default.

## Inheritance / interface lists

`: BaseType` on a new line, indented 4 spaces:

```csharp
public sealed class DynamoDbFixture
    : IAsyncLifetime
```

## Naming

No `Base` suffix on abstract/base types — use a meaningful name instead.
