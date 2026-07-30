---
title: Parity test — condition expression breadth
summary: Cross-backend assertions for ConditionExpression primitives beyond attribute_exists/not_exists and size — attribute_type, contains on string and list, begins_with inside ConditionExpression, and AND/OR/NOT trees.
tags: [note, todo, parity, v1.1]
created: 2026-05-27
---

# Parity test — condition expression breadth

Cross-backend assertions for `ConditionExpression` primitives beyond `attribute_exists`/`not_exists` and `size()` — `attribute_type`, `contains` on string and list, `begins_with` inside `ConditionExpression`, and AND/OR/NOT trees.

## What's missing

Today's parity suite covers `attribute_exists`, `attribute_not_exists` (in `ItemCrudParityTests`), and `size()` (in `SizeOperatorParityTests`). Missing primitives, each with a distinct parser path:

- `attribute_type(path, :type)` — type-check guard.
- `contains(field, :v)` on a String attribute (today's `contains` parity is on string sets only, which lands on a different SQL translation).
- `contains(field, :v)` on a List attribute.
- `begins_with(field, :v)` inside a `ConditionExpression` — parser path differs from `begins_with` in a `KeyConditionExpression`.
- Compound logic: `AND`, `OR`, `NOT`, parenthesized nesting (`((a AND b) OR (c AND d))`).

## Why parser-divergence risk

`ConditionExpression` is parsed and translated to SQL by [[docs/notes/parser-result-caching.md]]'s expression engine. Each missing primitive exercises code paths that the main suite hits in-proc but no parity test ever compares against real DynamoDB. Operator precedence and short-circuit semantics in compound trees are the highest-drift surfaces.

## Acceptance

Add `ConditionExpressionParityTests.cs` with `[Theory]` per primitive, each parameterized over the three backends. Each test:

- Puts a fixture item.
- Runs `UpdateItem` or `DeleteItem` with the condition.
- Asserts success or `ConditionalCheckFailedException` on the AWS-API contract — not on cross-client response equality (per the strategy in [[docs/notes/parity-coverage-status.md]]).

Cover at least one positive and one negative case per primitive, plus one compound-tree scenario combining `AND` / `OR` / `NOT` over mixed primitives.

## Sequencing

First in the [[docs/notes/parity-parser-divergence-test-set.md]] epic — broadest divergence surface.
