---
title: Parity test — Query BETWEEN on string sort key
summary: Cross-backend assertion that Query KeyConditionExpression with BETWEEN on a string-typed sort key matches real DynamoDB's lexicographic ordering; numeric BETWEEN is covered, the string parser path is not.
tags: [note, todo, parity, v1.1]
created: 2026-05-27
---

# Parity test — Query BETWEEN on string sort key

Cross-backend assertion that `Query` `KeyConditionExpression` with `BETWEEN` on a string-typed sort key matches real DynamoDB's lexicographic ordering; numeric `BETWEEN` is covered, the string parser path is not.

## What's missing

`QueryNumericSortKeyParityTests` covers `BETWEEN` against a numeric sort key. No parity test exercises `BETWEEN` against a string sort key, where the SQL builder lands on a different translation path — lexicographic `>= AND <=` instead of numeric range over the `sk_num` column.

## Why parser-divergence risk

Numeric and string sort keys land in separate columns in the SQLite schema (`sk_num` versus `sk`). `BETWEEN` on each goes through a different SQL builder branch. The main suite exercises both in-proc, but the cross-backend agreement on lexicographic edge cases — case sensitivity, embedded numerics in strings (`"item-9"` vs `"item-10"`), inclusive endpoints — is not asserted.

## Acceptance

Add cases to `QueryParityTests.cs` (or extend `QueryNumericSortKeyParityTests` into a `QueryBetweenParityTests` matrix):

- Seed items with string sort keys spanning the boundary (e.g., `"a"`, `"b"`, `"c"`, `"d"`).
- `Query` with `KeyConditionExpression = "pk = :p AND sk BETWEEN :lo AND :hi"`.
- Assert returned items match the inclusive lexicographic range across all three backends.
- Include an edge case where embedded digits would diverge between lex and numeric ordering (`"item-2"` vs `"item-10"`).

## Sequencing

Fourth in the [[docs/notes/parity-parser-divergence-test-set.md]] epic — small surface, but the two-column-typed-sort-key design makes silent drift plausible. Listed in the Query gap section of [[docs/notes/parity-coverage-gaps-in-operation-variants.md]].
