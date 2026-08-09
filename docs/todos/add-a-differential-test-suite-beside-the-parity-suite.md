---
title: Add a differential test suite beside the parity suite
type: todo
summary: "A new suite that issues one request to DdbLite and DynamoDbLocal side by side and compares whole responses, catching divergence no hand-written assertion names. Additive: the parity suite stays as it is."
tags: [parity, differential, testing]
created: 2026-08-08
priority: medium
status: open
cites: ["[[parity-not-differential]]", "[[parity-coverage-status]]"]
---

# Add a differential test suite beside the parity suite

Build a second suite that runs both backends against the same request and compares their responses. The parity suite stays as it is.

## Why

The parity suite replays hand-written expectations once per backend, so parity holds only on the properties an author named. Divergence in anything unasserted passes on every backend. [parity-not-differential](../notes/parity-not-differential.md) records the mechanism and its boundary.

A differential test also makes the reference backend structurally required. It cannot run with DynamoDbLocal excluded. A parity test excludes it and still goes green.

## Shape of the work

A new project, `tests/DynamoDbLite.Differential.Tests/`, following the container fixture in `tests/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs`. Each test acquires a DdbLite client and a DynamoDbLocal client, issues the identical request to both, and compares the responses structurally.

The comparison helper is the load-bearing piece. It walks `Dictionary<string, AttributeValue>` including the type discriminator on every value, so a divergence between `N` and `S`, or a dropped `NULL`, fails instead of comparing equal.

Two things need deciding at implementation time:

- Normalization. Responses carry values that cannot match — `CreationDateTime` and `TableArn` from `DescribeTable`, SDK response metadata and request IDs. Each exclusion is explicit and justified where it is written, never a blanket skip.
- Ordering. `Scan` result order is unspecified, so compare as sets. `Query` with `ScanIndexForward` has ordering in its contract, so compare as sequences. The helper takes which one applies.

Start with the read paths that return rich responses: `GetItem`, `Query`, `Scan`, `BatchGetItem`. Writes with `ReturnValues` after those.

## Acceptance

- Every test acquires both clients and sends the same request to both. A test taking a single backend belongs in the parity suite, not this one.
- The suite fails when the container is unavailable. Never add a skip-if-no-container guard, and never pass `--filter` to it.
- Validate with `build-gate.sh` solution-wide and unfiltered, per `csharp:writing-csharp`.
- Nothing under `tests/DynamoDbLite.Parity.Tests/` is deleted or weakened.
- Not closeable until two runs are recorded: an unfiltered run green, and a run where a deliberate mutation to a DdbLite response path turns the new suite red. The second proves the comparison can fail.
