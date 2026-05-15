# Parity Tests

Integration tests that prove DynamoDbLite matches real DynamoDB behavior. Each scenario runs three times — against `DynamoDbClient` (in-memory SQLite), `DynamoDbClient` (file-based SQLite, WAL), and `AmazonDynamoDBClient` pointed at `amazon/dynamodb-local` via Testcontainers — and must reach an explicit expected outcome on all three. Tracks Phase 14 in [`architecture-decisions.md`](architecture-decisions.md).

Tests live in `src/DynamoDbLite.Parity.Tests/`. The main test project stays container-free.

## Shape

A `ParityBackend` enum drives one axis of the test theory, mirroring the `StoreType` pattern in the main test project's [`DynamoDbClientFixture`](../src/DynamoDbLite.Tests/Fixtures/DynamoDbClientFixture.cs):

```csharp
public enum ParityBackend
{
    DdbLite,        // DynamoDbClient (in-memory SQLite)
    DdbLiteFile,    // DynamoDbClient (file-based SQLite, WAL)
    DynamoDbLocal,  // AmazonDynamoDBClient -> amazon/dynamodb-local container
}
```

The collection fixture at [`DynamoDbFixture`](../src/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs) owns one client per backend. Tests are `[Theory]` methods with one `[InlineData(ParityBackend.X)]` per backend, and resolve the client through `fixture.Client(backend)`. Adding a parity scenario costs one method body and three `InlineData` lines.

Three backends close the file-vs-memory drift surface; the in-memory and file-based stores have different concurrency strategies and have drifted before.

## Container Lifecycle

One container for the entire parity-test run, started in [`DynamoDbFixture.InitializeAsync`](../src/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs). Container start is the slow step; per-test container start would push the suite into the minute range.

Each test creates its own table with a unique name from `TestTables.UniqueName(prefix)` and leaves it. No per-test teardown — the fixture disposes the three backends at the end of the run, and accumulation across ~14 tests is negligible for SQLite and irrelevant for the container.

`DisposeAsync` order matters: the file-based `DynamoDbClient` disposes before `FileBasedTestHelper.Cleanup` runs, so the SQLite handle is closed before `File.Delete` touches the `.db` file. Skipping that order causes `File.Delete` to throw `IOException` on Windows.

## Assertion Strategy

Each test asserts an explicit expected outcome — item count, attribute values, status code, exception type — and runs the same body against each backend. Both must reach the same expectation.

Responses from the three clients are not compared to each other. Real DynamoDB and DynamoDbLite differ legitimately on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages. Stripping those fields would be more code than the tests themselves. A shared bug between implementations would also pass cross-comparison silently.

Tests document the contract. The expected outcome is written from the [AWS DynamoDB API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/), not from whichever implementation happens to be running.

## Error Parity

Production code branches on DynamoDB error codes. Tests assert on exception type, not message text:

- `ConditionalCheckFailedException` for failed condition expressions
- `ResourceNotFoundException` for missing tables and indexes
- `ValidationException` for malformed requests
- `TransactionCanceledException` with `CancellationReasons[i].Code` matching the failing item's index

This is the surface where `amazon/dynamodb-local` sometimes diverges from real DynamoDB. Tracking which behavior DynamoDbLite implements is a deliberate decision per scenario, not a default.

## Reserved Keywords

DynamoDB's [reserved word list](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) covers ~570 identifiers. `amazon/dynamodb-local` rejects unescaped reserved words in expressions; DynamoDbLite does not. Tests using attribute names like `name`, `counter`, `status`, or `count` must escape via `ExpressionAttributeNames` to avoid failing against the container backend. The reserved-name failure mode is real DynamoDB's behavior — DynamoDbLite's permissiveness is the gap.

## Coverage

Initial slice covers 14 scenarios across seven test files, mapping one-to-one against parity claims in `README.md`:

- [`ItemCrudParityTests`](../src/DynamoDbLite.Parity.Tests/ItemCrudParityTests.cs) — `PutItem` + `GetItem` round-trip across S/N/BOOL/L/M; `attribute_not_exists` condition on `PutItem` (success and `ConditionalCheckFailedException`); `attribute_exists` condition on `DeleteItem` failure.
- [`UpdateExpressionParityTests`](../src/DynamoDbLite.Parity.Tests/UpdateExpressionParityTests.cs) — `SET` with `if_not_exists`, `ADD` on number, `REMOVE`.
- [`QueryParityTests`](../src/DynamoDbLite.Parity.Tests/QueryParityTests.cs) — `KeyConditionExpression`; `ScanIndexForward = false`; `Limit` + `LastEvaluatedKey` pagination.
- [`ScanParityTests`](../src/DynamoDbLite.Parity.Tests/ScanParityTests.cs) — `FilterExpression` with correct `Count` and `ScannedCount`.
- [`TransactionParityTests`](../src/DynamoDbLite.Parity.Tests/TransactionParityTests.cs) — `TransactWriteItems` all-or-nothing rollback with `CancellationReasons[i].Code == "ConditionalCheckFailed"`.
- [`BatchParityTests`](../src/DynamoDbLite.Parity.Tests/BatchParityTests.cs) — `BatchGetItem` happy path.
- [`SecondaryIndexParityTests`](../src/DynamoDbLite.Parity.Tests/SecondaryIndexParityTests.cs) — GSI query with `INCLUDE` projection returns projected attributes only.

14 scenarios × 3 backends = 42 test executions per parity run.

## Configuration

Testcontainers reads its endpoint and Ryuk settings from `~/.testcontainers.properties` per developer. The Podman-on-Windows setup needs:

```
docker.host=npipe://./pipe/podman-machine-default
ryuk.disabled=true
```

Docker Desktop users need no file — defaults work. Linux runners (CI) use the native `/var/run/docker.sock` automatically; no env vars needed. Hardcoding the endpoint in C# would break mixed-environment teams; the per-user file is the right knob.

## Next

### Scenarios to add

Mapped to README parity claims, in rough priority order:

- **Attribute types not yet round-tripped:** B (binary), NULL, SS, NS, BS. Real DynamoDB once rejected empty strings; current behavior allows them — worth a scenario.
- **Condition operators not yet exercised:** `begins_with`, `contains`, `size`, `between`, `IN`.
- **Update operators not yet exercised:** `DELETE` (set element removal), `list_append`.
- **Numeric sort key:** all current tests use string SK. Numeric SK ordering and range comparisons need their own coverage (use `TestTables.HashKeyStringSortKeyNumber`).
- **LSI:** GSI coverage exists; LSI is structurally different (shared partition, table-level provisioning) and untested.
- **GSI projection variants:** `KEYS_ONLY` and `ALL` (currently only `INCLUDE`).
- **`BatchWriteItem`:** put and delete in one batch, mixed-table batches.
- **`TransactGetItems`:** read-side transactional consistency.
- **More transaction failure modes:** multiple simultaneous condition failures, `ClientRequestToken` idempotency, `ReturnValuesOnConditionCheckFailure`.
- **`Select.COUNT`** on Query and Scan.
- **`ReturnValues`** variants on `PutItem`, `UpdateItem`, `DeleteItem` (`ALL_OLD`, `UPDATED_NEW`, etc.).
- **Pagination edge cases:** scan with `Segment`/`TotalSegments`, query that exits via `Limit` versus exits via end-of-data.

### Library gaps found by parity tests

Tracked here until either fixed or accepted as known limitations.

- **Reserved keyword permissiveness.** DynamoDbLite accepts reserved words in `UpdateExpression`/`ConditionExpression`/`KeyConditionExpression`/`FilterExpression` without `ExpressionAttributeNames` escaping. Real DynamoDB and `amazon/dynamodb-local` reject them. Discovered via the `ADD counter :inc` test in [`UpdateExpressionParityTests`](../src/DynamoDbLite.Parity.Tests/UpdateExpressionParityTests.cs). The expression parser at `src/DynamoDbLite/Expressions/` would gain a reserved-word check at parse time.

### Deferred indefinitely

Out of scope per [`architecture-decisions.md`](architecture-decisions.md) or because the test surface is hostile:

- **Real AWS DynamoDB (cloud) backend.** Requires credentials, costs money, network-dependent. The three local backends cover the contract.
- **TTL parity.** `amazon/dynamodb-local` runs TTL on a long cron; expiration windows make CI-friendly tests impractical.
- **Export/Import.** Out of scope per `architecture-decisions.md` — local file-based emulator semantics differ from S3 anyway.
- **Cross-client response-shape equality.** Replaced by explicit expected outcomes per the assertion strategy above.
