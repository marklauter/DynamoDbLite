---
title: CA1816 — DisposeAsync should call GC.SuppressFinalize
summary: Two test base classes implement DisposeAsync without suppressing finalization; add GC.SuppressFinalize(this).
tags: [todo, remediation, ddblite, analyzers, tests, ca1816]
created: 2026-07-16
priority: high
effort: low
status: open
---

CA1816 ("call GC.SuppressFinalize correctly") fires on two `DisposeAsync`
implementations in test base classes. Without the call, a derived type that
introduces a finalizer would keep being finalized after disposal.

## Fix

Real fix — add the suppression call inside `DisposeAsync`:

```csharp
public async ValueTask DisposeAsync()
{
    await DisposeAsyncCore();
    GC.SuppressFinalize(this);
}
```

Confirm the exact shape against the existing method bodies before editing —
`ExportTestsBase` / `ImportTestsBase` may dispose owned resources first.

## Blocking sites (2)

- tests/DynamoDbLite.Tests/ExportTests.cs:52 (ExportTestsBase.DisposeAsync)
- tests/DynamoDbLite.Tests/ImportTests.cs:83 (ImportTestsBase.DisposeAsync)
