---
title: CA1515 — test classes flagged as internalizable
summary: The canonical .editorconfig raised CA1515 to error; xUnit requires public test classes, so the fix is the house test-project carve-out, not internalizing.
tags: [todo, remediation, ddblite, analyzers, tests, ca1515]
created: 2026-07-16
priority: high
effort: low
status: open
---

CA1515 ("types can be made internal") is an **error** since the canonical
`.editorconfig` landed in #98. xUnit discovers `[Fact]`/`[Theory]` methods on
**public** classes, so the rule fights a framework requirement — this is a false
positive for test projects, not a real defect.

## Fix

Adopt the house test-project carve-out. pool and lexi both suppress CA1515 in the
`.Tests` `PropertyGroup` of `Directory.Build.props`:

```xml
<!-- CA1515 - xUnit requires public test classes, so "make public types internal" does not apply. -->
<NoWarn>$(NoWarn);...;CA1515</NoWarn>
```

DynamoDbLite's carve-out (`Directory.Build.props`, the
`MSBuildProjectName.EndsWith('.Tests')` group) currently lists
`CA1707;CA1051;IDISP026;IDE1006` — CA1515 was never added. Append it.

One line clears every occurrence. `dotnet build` reports 7 explicit sites; `dotnet
format` reports ~64 (every public test class). The suppression resolves all of
them regardless of count.

## Blocking sites (build-reported)

- tests/DynamoDbLite.Parity.Tests/Fixtures/BackendDataAttribute.cs:10
- tests/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixtureCollection.cs:7
- tests/DynamoDbLite.Parity.Tests/Fixtures/ParityBackend.cs:3
- tests/DynamoDbLite.Tests/ExportTests.cs:258, :264
- tests/DynamoDbLite.Tests/ImportTests.cs:430, :436

See [[06-align-ci-path-filter-to-canon]] for why this went unnoticed.
