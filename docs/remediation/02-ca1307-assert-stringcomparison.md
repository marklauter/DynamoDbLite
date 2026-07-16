---
title: CA1307 — Assert.Contains/StartsWith without StringComparison
summary: 36 string-assert calls in the test suite pick a culture-dependent overload; add an explicit StringComparison.Ordinal to each.
tags: [todo, remediation, ddblite, analyzers, tests, ca1307]
created: 2026-07-16
priority: high
effort: medium
status: open
---

CA1307 ("specify StringComparison for clarity") is an **error** since #98. The
xUnit `Assert.Contains(string, string?)` and `Assert.StartsWith(string?, string?)`
overloads resolve to a culture-dependent comparison; the analyzer wants the intent
stated.

## Fix

This is a real fix, not a suppression — the house repos do not carve CA1307 out.
Add `StringComparison.Ordinal` to each flagged call:

```csharp
Assert.Contains("expected", actual, StringComparison.Ordinal);
Assert.StartsWith("prefix", actual, StringComparison.Ordinal);
```

Ordinal is correct here: these assert on error messages and generated IDs, not
human-facing text. 36 sites across 10 files — mechanical but must be done per call.

## Blocking sites (36)

- BatchOperationsTests.cs:296, :334, :670, :698, :739
- BatchWriteLimitTests.cs:85
- ExportTests.cs:79, :141, :211
- Expressions/ConditionExpressionTests.cs:603, :604, :605
- Expressions/KeyConditionExpressionTests.cs:117, :118
- Expressions/LegacyConditionConverterTests.cs:194
- Expressions/ProjectionExpressionTests.cs:343, :344
- Expressions/UpdateExpressionTests.cs:289, :307, :328, :553, :578, :631, :632, :633
- QueryTests.cs:219
- TagTests.cs:180, :195, :210, :225, :240, :254
- TimeToLiveTests.cs:88, :137, :153, :169
