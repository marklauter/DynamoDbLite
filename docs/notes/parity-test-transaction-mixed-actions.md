---
title: Parity test — transaction mixed actions and item-limit rejection
summary: Cross-backend assertion that a single TransactWriteItems can mix Put + Update + Delete + ConditionCheck actions, and that the 100-item cap is enforced uniformly with ValidationException.
tags: [note, todo, parity, v1.1]
created: 2026-05-27
---

# Parity test — transaction mixed actions and item-limit rejection

Cross-backend assertion that a single `TransactWriteItems` can mix `Put` + `Update` + `Delete` + `ConditionCheck` actions, and that the 100-item cap is enforced uniformly with `ValidationException`.

## What's missing

`TransactionParityTests` covers all-or-nothing rollback, `CancellationReasons[i].Code`, `ClientRequestToken` idempotency, and `ReturnValuesOnConditionCheckFailure = ALL_OLD` — all with single-action-type transactions. Missing:

- A single transaction containing all four action types: `Put`, `Update`, `Delete`, `ConditionCheck`. Asserts the executor handles heterogeneous batches and orders side effects so any per-item condition failure rolls the whole transaction back.
- `>100` `TransactItems` rejected with `ValidationException` across all three backends — DynamoDB's transaction item cap.
- (Optional) `>100` `TransactGetItems` rejected the same way.

## Why parser-divergence risk

Mixed-action transactions exercise the action dispatcher and rollback path that single-action-type tests don't reach. If DdbLite's transaction executor accumulates state per action type rather than per item, a mixed batch could land in an inconsistent state on rollback without the existing tests catching it. The item-cap rejection is also a contract surface: silently accepting an over-cap batch would be a meaningful drop-in divergence.

## Acceptance

Add to `TransactionParityTests.cs`:

- Mixed-action happy path: one transaction with one of each action, assert post-state across all touched items.
- Mixed-action condition failure: one transaction with one of each action where the `ConditionCheck` fails, assert no side effects on the other three items.
- Item-cap rejection: 101-item `TransactWriteItems`, assert `ValidationException` across the three backends.

## Sequencing

Fifth in the [[docs/notes/parity-parser-divergence-test-set.md]] epic — lower parser risk than expression-shape items but a real semantic surface. Listed in the Transactions gap section of [[docs/notes/parity-coverage-gaps-in-operation-variants.md]].
