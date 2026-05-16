# ADR 0004 — Behavioral Fidelity

Status: Accepted

Full fidelity with DynamoDB semantics:

- Parse and evaluate `ConditionExpression`, `FilterExpression`, `KeyConditionExpression`, `ProjectionExpression`
- Support `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`)
- Enforce key schema validation
