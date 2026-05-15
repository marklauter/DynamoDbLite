# Parity Tests

Integration tests that prove DynamoDbLite matches real DynamoDB behavior. Each scenario runs three times — against `DynamoDbClient` (in-memory SQLite), `DynamoDbClient` (file-based SQLite, WAL), and `AmazonDynamoDBClient` pointed at `amazon/dynamodb-local` via Testcontainers — and must reach an explicit expected outcome on all three. Tracks Phase 14 in [`architecture-decisions.md`](architecture-decisions.md).

Tests live in `test/DynamoDbLite.Parity.Tests/`. The main test project stays container-free.

## Shape

A `ParityBackend` enum drives one axis of the test theory, mirroring the `StoreType` pattern in the main test project's [`DynamoDbClientFixture`](../test/DynamoDbLite.Tests/Fixtures/DynamoDbClientFixture.cs):

```csharp
public enum ParityBackend
{
    DdbLite,        // DynamoDbClient (in-memory SQLite)
    DdbLiteFile,    // DynamoDbClient (file-based SQLite, WAL)
    DynamoDbLocal,  // AmazonDynamoDBClient -> amazon/dynamodb-local container
}
```

The collection fixture at [`DynamoDbFixture`](../test/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs) owns one client per backend. Tests are `[Theory]` methods with one `[InlineData(ParityBackend.X)]` per backend, and resolve the client through `fixture.Client(backend)`. Adding a parity scenario costs one method body and three `InlineData` lines.

Three backends close the file-vs-memory drift surface; the in-memory and file-based stores have different concurrency strategies and have drifted before.

## Container Lifecycle

One container for the entire parity-test run, started in [`DynamoDbFixture.InitializeAsync`](../test/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs). Container start is the slow step; per-test container start would push the suite into the minute range.

Each test creates its own table with a unique name from `TestTables.UniqueName(prefix)` and leaves it. No per-test teardown — the fixture disposes the three backends at the end of the run, and accumulation across the suite is negligible for SQLite and irrelevant for the container.

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

DynamoDB's [reserved word list](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) covers ~570 identifiers. Real DynamoDB and `amazon/dynamodb-local` reject unescaped reserved words in expressions. DynamoDbLite matches: each parser checks raw identifiers against [`DynamoDbReservedWords`](../src/DynamoDbLite/Expressions/DynamoDbReservedWords.cs) and throws `ArgumentException` on a match. Names escaped via `ExpressionAttributeNames` (`#name`) bypass the check, as in real DynamoDB.

## Coverage

Scenarios map one-to-one against parity claims in `README.md`:

- [`ItemCrudParityTests`](../test/DynamoDbLite.Parity.Tests/ItemCrudParityTests.cs) — `PutItem` + `GetItem` round-trip across S/N/BOOL/L/M, plus B/NULL/SS/NS/BS round-trips; `attribute_not_exists` condition on `PutItem` (success and `ConditionalCheckFailedException`); `attribute_exists` condition on `DeleteItem` failure.
- [`UpdateExpressionParityTests`](../test/DynamoDbLite.Parity.Tests/UpdateExpressionParityTests.cs) — `SET` with `if_not_exists`, `SET` with `list_append`, `ADD` on number, `REMOVE`, `DELETE` on string set.
- [`QueryParityTests`](../test/DynamoDbLite.Parity.Tests/QueryParityTests.cs) — `KeyConditionExpression`; `begins_with` on sort key; `ScanIndexForward = false`; `Limit` + `LastEvaluatedKey` pagination.
- [`QueryNumericSortKeyParityTests`](../test/DynamoDbLite.Parity.Tests/QueryNumericSortKeyParityTests.cs) — `BETWEEN` on numeric sort key returns inclusive range in ascending order.
- [`ScanParityTests`](../test/DynamoDbLite.Parity.Tests/ScanParityTests.cs) — `FilterExpression` with correct `Count` and `ScannedCount`; `contains` on string set; `IN` against a value list; parallel scan with `Segment`/`TotalSegments`.
- [`TransactionParityTests`](../test/DynamoDbLite.Parity.Tests/TransactionParityTests.cs) — `TransactWriteItems` all-or-nothing rollback with `CancellationReasons[i].Code == "ConditionalCheckFailed"`; multiple simultaneous condition failures populate each index; `ClientRequestToken` idempotency on replay; `ReturnValuesOnConditionCheckFailure = ALL_OLD` includes the prior item.
- [`TransactGetItemsParityTests`](../test/DynamoDbLite.Parity.Tests/TransactGetItemsParityTests.cs) — `TransactGetItems` happy path across two tables in request order; missing key returns empty `Item` at that response index without throwing.
- [`SelectCountParityTests`](../test/DynamoDbLite.Parity.Tests/SelectCountParityTests.cs) — `Select = COUNT` on Query and Scan populates `Count`/`ScannedCount` and returns no items.
- [`ReturnValuesParityTests`](../test/DynamoDbLite.Parity.Tests/ReturnValuesParityTests.cs) — `ReturnValues` variants across `PutItem` (`ALL_OLD`, `NONE`), `UpdateItem` (`ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`), and `DeleteItem` (`ALL_OLD`).
- [`ExpressionValidationOrderParityTests`](../test/DynamoDbLite.Parity.Tests/ExpressionValidationOrderParityTests.cs) — raw reserved words in expressions are rejected with `ValidationException` *before* any lookup or mutation, across `DeleteItem`, `Query`, `Scan`, `TransactWriteItems`, `TransactGetItems`, and `BatchGetItem`.
- [`BatchParityTests`](../test/DynamoDbLite.Parity.Tests/BatchParityTests.cs) — `BatchGetItem` happy path; `BatchWriteItem` with put + delete in a single batch; `BatchWriteItem` across two tables.
- [`SecondaryIndexParityTests`](../test/DynamoDbLite.Parity.Tests/SecondaryIndexParityTests.cs) — GSI query across projection variants: `INCLUDE` returns projected attributes only; `KEYS_ONLY` returns only table + index keys; `ALL` returns every attribute.
- [`LocalSecondaryIndexParityTests`](../test/DynamoDbLite.Parity.Tests/LocalSecondaryIndexParityTests.cs) — LSI query with `begins_with` on the alternate sort key; `INCLUDE` projection returns projected attributes only.
- [`ReservedWordParityTests`](../test/DynamoDbLite.Parity.Tests/ReservedWordParityTests.cs) — raw reserved words in `UpdateExpression`/`ConditionExpression`/`ProjectionExpression` throw `AmazonDynamoDBException` with `ErrorCode == "ValidationException"`; the same word escaped via `ExpressionAttributeNames` is accepted.
- [`EmptyStringParityTests`](../test/DynamoDbLite.Parity.Tests/EmptyStringParityTests.cs) — empty-string scalar values round-trip through `PutItem` + `GetItem` (real DynamoDB rejected these pre-2020; current behavior accepts them).
- [`SizeOperatorParityTests`](../test/DynamoDbLite.Parity.Tests/SizeOperatorParityTests.cs) — `size()` in `ConditionExpression` on `UpdateItem` (success and `ConditionalCheckFailedException`); `size()` in `FilterExpression` on `Scan`.

## Configuration

Testcontainers reads its endpoint and Ryuk settings from `~/.testcontainers.properties` per developer. The Podman-on-Windows setup needs:

```
docker.host=npipe://./pipe/podman-machine-default
ryuk.disabled=true
```

Docker Desktop users need no file — defaults work. Linux runners (CI) use the native `/var/run/docker.sock` automatically; no env vars needed. Hardcoding the endpoint in C# would break mixed-environment teams; the per-user file is the right knob.

## Running selectively

Every parity test is parameterized through [`BackendDataAttribute`](../test/DynamoDbLite.Parity.Tests/Fixtures/BackendDataAttribute.cs), which tags each row with a `Backend` trait. The fixture starts `amazon/dynamodb-local` lazily, so a run that never requests `DynamoDbLocal` never spins up a container.

```
dotnet test test/DynamoDbLite.Parity.Tests --filter "Backend=DdbLite"           # in-memory only
dotnet test test/DynamoDbLite.Parity.Tests --filter "Backend=DdbLiteFile"       # file-backed SQLite only
dotnet test test/DynamoDbLite.Parity.Tests --filter "Backend=DynamoDbLocal"     # real DynamoDB Local
dotnet test test/DynamoDbLite.Parity.Tests --filter "Backend=DdbLite|Backend=DdbLiteFile"   # both lite backends, no container
```

Wall-clock difference matters for the inner-dev loop — the container is the bulk of the cost, so filtering to lite backends roughly halves run time. CI should always run the full matrix (no filter).

## Next

### Scenarios to add

The initial planned scenarios — mapped to README parity claims — are covered. New scenarios land here as new claims appear or as the **Library gaps** below close and prompt follow-on coverage.

### Parity benchmarks

Worth a dedicated `DynamoDbLite.Parity.Benchmarks` project (BenchmarkDotNet) that runs the same operations across the three backends so we can quantify the speed gap, not just the behavioral parity. Useful for: justifying DynamoDbLite for hot-path test workloads, catching perf regressions in the SQLite store, and giving developers a real number behind "the lite backends are much faster than the container." Suggested coverage: single-item Get/Put, bulk write of 100 items, Query with Limit, parallel Scan.

### Library gaps found by parity tests

Tracked here until either fixed or accepted as known limitations.

_None currently open._

### Deferred indefinitely

Out of scope per [`architecture-decisions.md`](architecture-decisions.md) or because the test surface is hostile:

- **Real AWS DynamoDB (cloud) backend.** Requires credentials, costs money, network-dependent. The three local backends cover the contract.
- **TTL parity.** `amazon/dynamodb-local` runs TTL on a long cron; expiration windows make CI-friendly tests impractical.
- **Export/Import.** Out of scope per `architecture-decisions.md` — local file-based emulator semantics differ from S3 anyway.
- **Cross-client response-shape equality.** Replaced by explicit expected outcomes per the assertion strategy above.
