---
title: Parity, not differential
type: note
summary: "The suite runs one hand-written expectation against every backend rather than comparing outputs; red on DynamoDbLocal means the expectation is wrong, red on DdbLite means DdbLite is wrong."
tags: [parity, testing, dynamodb-local]
created: 2026-08-08
status: evolving
cites: ["[[parity-with-dynamodb-local]]", "[[parity-coverage-status]]"]
---

# Parity, not differential

Our parity tests add DynamoDbLocal to our test sweep, and we adjust the tests to meet DynamoDbLocal expectations, then adjust our DdbLite code to conform if the tests go red. So a red test against DynamoDbLocal means the test is wrong. A red test against DdbLite means DdbLite is wrong. So we have parity, but not differential.

## How that shows up in the code

`BackendDataAttribute` emits one row per `ParityBackend`. Every test method under `tests/DynamoDbLite.Parity.Tests/` takes a single `ParityBackend backend` and holds a single client. No test compares one backend's response against another's at run time. The comparison happens when the assertion is written, not when it runs.

## What the claim covers

Parity holds on the properties the assertions name, and says nothing outside them. `ScanParityTests.Scan_with_Limit_paginates_via_LastEvaluatedKey_without_duplicates_or_gaps` pins the page count, each page's item and scanned counts, every cursor's contents, and the returned key set. Scan-pagination parity means those properties and no others.

Every assertion in that file projects a returned item down to `item["PK"].S`. The `group` attribute each seeded item carries is never read back, so a backend that dropped it from scan results, or returned it under a different type, passes every case there.

## Repairing a test that went red on the reference

That repair can weaken an assertion with nothing going red to signal the loss. An expectation loosened until DynamoDbLocal passes also stops catching the DdbLite bugs it caught before. The repair makes the expectation more specific to what DynamoDbLocal did, never more permissive.

## Consequence for running the suite

A test stands up without DynamoDbLocal, so filtering that backend out yields a green suite that carries no parity evidence. Under a differential design the test cannot execute without both clients, so a filtered run fails instead of passing. Moving to differential comparison is an open question. It restructures the fixture and every file in the suite. Responses also carry values that cannot match across backends, such as the table creation timestamp `DescribeTable` returns, so a comparison needs explicit normalization.
