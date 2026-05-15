# Parser-result caching

Tags: release, v1.1, performance, parser, bench-needed
Cache parsed expression ASTs keyed by raw expression text; converts per-call parse cost into a one-time cost per distinct expression.


## Observation

`ConditionExpression`, `UpdateExpression`, `KeyConditionExpression`, `FilterExpression`, and `ProjectionExpression` are parsed on every API call. Real workloads call `PutItem` / `UpdateItem` with the same expression text many times — once per item in a batch, once per row in a sweep, once per request in a hot loop. Each call goes through `ConditionExpressionParser.Parse` / `UpdateExpressionParser.Parse` / `ProjectionExpressionParser.Parse` from scratch.

The user observed empirically that SQLite writes are ~10-20 ms with the indexes shipped in commit `6c6a8ca` — and that the parser dominates the rest. See [dynamodblite-write-path-is-slower-than-read-path](dynamodblite-write-path-is-slower-than-read-path.md).

## Interpretation

Caching parser results by raw expression text is the obvious lever. The cache key is the expression string; the value is the AST. Cache lookup is O(string-hash); on a hit, parsing cost goes from "tokenize + parse + AST allocations" to a dictionary read. On a miss, the cost is the same as today.

The same expression text reused across many calls (the common case) becomes a single parse for the application's lifetime. The cache should be bounded — applications can technically generate an unbounded number of distinct expression strings — but a default cap of a few thousand entries with LRU eviction handles realistic workloads.

The risk is shared cache state across `DynamoDbClient` instances; instance-local is safer than static. The cache should also not hold strong references to attribute names / values from `ExpressionAttributeNames` / `ExpressionAttributeValues`, since those vary per call even when the expression text is identical.

## Next

Sequence:

1. Land the bench project (see [parity-benchmarks-project](parity-benchmarks-project.md)) so a "before" number exists.
2. Pick one parser to start with — `ConditionExpressionParser.Parse` is the most-called.
3. Add a `ConcurrentDictionary<string, ConditionNode>` keyed by raw expression text, with size cap and LRU eviction. Instance-local on the `DynamoDbClient` (or its `SqliteStore`).
4. Verify the AST is genuinely immutable (read-only); if any node carries mutable state, the cache is unsafe.
5. Re-run the bench. If the gain is real, propagate to the other parsers.

If the AST has any mutable fields, the cache returns a structural template and per-call evaluation binds attribute names/values at evaluate-time — which is already the existing pattern in `ConditionExpressionEvaluator.Evaluate`.
