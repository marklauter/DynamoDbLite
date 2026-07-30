---
title: Remaining coverage gaps after v1.0 cleanup
type: note
summary: "Honest-test coverage in DynamoDbLite.Tests is exhausted; the uncovered lines that remain are defensive throws, background error arms, half-implemented edge cases, dead code, and time-dependent paths. None warrant a new test."
tags: [coverage, known-limitation, dead-code, defensive-throws]
created: 2026-05-16
status: evolving
constrained-by: "[[no-background-work]]"
---

# Remaining coverage gaps after v1.0 cleanup

Honest-test coverage in `DynamoDbLite.Tests` is exhausted at ~97% line / ~87% branch. The remaining uncovered lines fall into five categories. None warrant a new test.

## Observation

After the bucket-C/bucket-D pass on the v1.0 surface, ~120 uncovered lines remain. They classify as:

### 1. Defensive throws — "unreachable from parser" / closed enum (~50 lines)

Self-marked with `// defensive: unreachable from parser` (or equivalent shape). The type system or upstream parser guarantees the arm is dead:

- `ConditionExpressionEvaluator.cs:21, 36, 37, 59, 71-72, 76, 124, 167, 182`
- `ExpressionHelper.cs:49, 89, 107, 124, 154, 170, 175`
- `Query.cs:262, 291, 449`
- `Transactions.cs:105, 402`
- `DynamoDbReservedWords.cs:71`
- `SqliteStore.cs:197`

Bucket-B policy (deferred this cycle): leave them visible in the denominator rather than `[ExcludeFromCodeCoverage]` the gate away. Revisit when the cost of carrying the noise outweighs the structural-defense value.

### 2. Background error-handling paths (~25 lines)

`try` / `catch` fallbacks in fire-and-forget background tasks. Triggering them deterministically requires either fault injection or contrived setups:

- `Export.cs:94-96, 98` — write-side error during export, then `UpdateExportStatusAsync("FAILED")` itself fails.
- `Import.cs:130-147` — same shape on the import side.

The happy path is covered. The error-arm-of-the-error-arm is not.

### 3. Half-implemented edge cases (~10 lines)

- `ExpressionHelper.cs:84` — `SetAtPath` extends a list with `{ NULL = true }` placeholders when the index is beyond the list length. The next iteration reads `current.M` from a NULL placeholder, which is `null`, so a subsequent property access NREs. The path is partially implemented. A test that exercises line 84 also surfaces the latent bug. Not in scope for this cycle.

### 4. Dead code (~10 lines)

- `SqliteStore.cs:1132-1143` — `UpdateIndexMetadataAsync(string, List<IndexDefinition>, CancellationToken)`. Defined but never called; the live callers use `UpdateIndexMetadataInTransactionAsync` (private static, with a transaction). Candidate for deletion in a separate cleanup pass.

### 5. Time-dependent paths (~1 line)

- `Transactions.cs:320` — `PurgeExpiredTokens` inner `TryRemove` branch fires only when a token's `Expiry` has elapsed. Token TTL is hardcoded at `DateTime.UtcNow.AddMinutes(10)` (line 243). Testing requires either `TimeProvider` injection or reflection. Defer until either the seam exists or time-injection is added more broadly.

## Interpretation

The coverage gate is at 97.15% line / 86.89% branch / 98.06% method. The remaining gap is structural. Pursuing it further means either touching production code (extract dead, inject TimeProvider, fix the list-extend bug) or escaping the gate with `[ExcludeFromCodeCoverage]`. Neither is free, and neither is a coverage problem.

## Next

- If we want to ratchet the coverage thresholds (`Directory.Build.props` MSBuild properties), 97/86/98 are the current achievable ceiling without product-code changes. Set thresholds at those numbers (or one point below as a buffer).
- Consider a follow-up cleanup that:
  - Deletes `UpdateIndexMetadataAsync` (or wire it to a caller if intended).
  - Decides on `[ExcludeFromCodeCoverage]` for the bucket-1 defensive throws. Settle it in a single conversation.
  - Adds a `TimeProvider` injection point so `Transactions.cs:320` becomes testable alongside other time-dependent code paths.
- The half-implemented list-extend in `ExpressionHelper.cs` deserves its own note if pursued — file separately when it surfaces in a real scenario.
