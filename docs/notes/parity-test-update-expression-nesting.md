---
title: Parity test — update expression nesting
summary: Cross-backend assertions for UpdateExpression shapes beyond single-clause scalar SET/ADD/REMOVE/DELETE — nested map paths, list-index assignment, multi-clause SET, ADD/DELETE on number and string sets.
tags: [note, todo, parity, v1.1]
created: 2026-05-27
---

# Parity test — update expression nesting

Cross-backend assertions for `UpdateExpression` shapes beyond single-clause scalar `SET`/`ADD`/`REMOVE`/`DELETE` — nested map paths, list-index assignment, multi-clause `SET`, `ADD`/`DELETE` on number and string sets.

## What's missing

Today's `UpdateExpressionParityTests` covers `SET if_not_exists`, `SET list_append`, `ADD` on a number scalar, `REMOVE` of a single path, and `DELETE` on a string set. Missing variants:

- Nested map updates: `SET map.field = :v`, `SET map.inner.field = :v`.
- List-index assignment: `SET list[0] = :v`, `SET list[3] = :v` with sparse extension.
- Multi-clause: `SET a = :a, b = :b, c = :c`, mixed `SET` + `REMOVE` in one expression.
- `ADD` on a number set (today only number scalar).
- `ADD` on a string set.
- `REMOVE` of multiple paths in one expression.
- `DELETE` on a number set (today only string set).

## Why parser-divergence risk

Each variant exercises a distinct branch in the update-expression parser and a different SQL translation. Nested-map and list-index paths in particular interact with JSON path handling in the SQLite store — a surface the main suite tests in-proc but no parity test cross-checks.

## Acceptance

Extend `UpdateExpressionParityTests.cs` with one `[Theory]` per missing variant, parameterized over the three backends. Each test:

- Puts a fixture item with the relevant attribute shape (map, list, set).
- Runs `UpdateItem` with the expression.
- Reads the item back and asserts the AWS-API-contract post-state (which fields exist, types, values), not cross-client response equality.

## Sequencing

Second in the [[docs/notes/parity-parser-divergence-test-set.md]] epic — parser depth and JSON-path interaction make this near-equivalent in risk to condition-expression breadth. Listed as the parser-variant pillar in [[docs/notes/parity-coverage-gaps-in-operation-variants.md]].
