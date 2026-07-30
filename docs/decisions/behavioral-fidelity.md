---
title: Behavioral Fidelity
type: decision
summary: "Full fidelity with DynamoDB semantics: `ConditionExpression`, `FilterExpression`, `KeyConditionExpression`, and `ProjectionExpression` are parsed and evaluated, `UpdateExpression` supports `SET`, `REMOVE`, `ADD`, and `DELETE`, and key schema validation is enforced."
tags: [fidelity, expressions, key-schema]
created: 2026-05-16
status: locked
---

# Behavioral Fidelity

Status: Accepted

Full fidelity with DynamoDB semantics:

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `KeyConditionExpression`, `ProjectionExpression`
- Support `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`)
- Enforce key schema validation
