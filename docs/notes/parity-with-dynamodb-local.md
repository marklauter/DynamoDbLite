# Parity with dynamodb-local

Tags: parity, coverage, reference, dynamodb-local
Authoritative file-by-file list of every scenario the parity test suite asserts across DdbLite, DdbLiteFile, and amazon/dynamodb-local.


The suite under [`tests/DynamoDbLite.Parity.Tests/`](../../tests/DynamoDbLite.Parity.Tests/) runs each scenario three times — once per backend — and asserts the same explicit AWS-API-contract outcome on each. The main test project stays container-free; only this suite touches `amazon/dynamodb-local`. Tracks Phase 14 in [`docs/adrs/index.md`](../adrs/index.md). Closed library gaps and the v1.0 audit history live in [`parity-coverage-status.md`](parity-coverage-status.md).

## Backends

- `DdbLite` — `DynamoDbClient` over in-memory SQLite (`Mode=Memory;Cache=Shared`, GUID-keyed per fixture instance).
- `DdbLiteFile` — `DynamoDbClient` over file-backed SQLite in WAL mode.
- `DynamoDbLocal` — `AmazonDynamoDBClient` against the `amazon/dynamodb-local` container, started lazily by Testcontainers.

## Covered

- [`ItemCrudParityTests`](../../tests/DynamoDbLite.Parity.Tests/ItemCrudParityTests.cs) — `PutItem` + `GetItem` round-trip across S/N/BOOL/L/M, plus B/NULL/SS/NS/BS round-trips; `attribute_not_exists` condition on `PutItem` (success and `ConditionalCheckFailedException`); `attribute_exists` condition on `DeleteItem` failure.
- [`UpdateExpressionParityTests`](../../tests/DynamoDbLite.Parity.Tests/UpdateExpressionParityTests.cs) — `SET` with `if_not_exists`, `SET` with `list_append`, `ADD` on number, `REMOVE`, `DELETE` on string set.
- [`QueryParityTests`](../../tests/DynamoDbLite.Parity.Tests/QueryParityTests.cs) — `KeyConditionExpression`; `begins_with` on sort key; `ScanIndexForward = false`; `Limit` + `LastEvaluatedKey` pagination.
- [`QueryNumericSortKeyParityTests`](../../tests/DynamoDbLite.Parity.Tests/QueryNumericSortKeyParityTests.cs) — `BETWEEN` on numeric sort key returns inclusive range in ascending order.
- [`ScanParityTests`](../../tests/DynamoDbLite.Parity.Tests/ScanParityTests.cs) — `FilterExpression` with correct `Count` and `ScannedCount`; `contains` on string set; `IN` against a value list; parallel scan with `Segment`/`TotalSegments`.
- [`TransactionParityTests`](../../tests/DynamoDbLite.Parity.Tests/TransactionParityTests.cs) — `TransactWriteItems` all-or-nothing rollback with `CancellationReasons[i].Code == "ConditionalCheckFailed"`; multiple simultaneous condition failures populate each index; `ClientRequestToken` idempotency on replay; `ReturnValuesOnConditionCheckFailure = ALL_OLD` includes the prior item.
- [`TransactGetItemsParityTests`](../../tests/DynamoDbLite.Parity.Tests/TransactGetItemsParityTests.cs) — `TransactGetItems` happy path across two tables in request order; missing key returns empty `Item` at that response index without throwing.
- [`SelectCountParityTests`](../../tests/DynamoDbLite.Parity.Tests/SelectCountParityTests.cs) — `Select = COUNT` on Query and Scan populates `Count`/`ScannedCount` and returns no items.
- [`ReturnValuesParityTests`](../../tests/DynamoDbLite.Parity.Tests/ReturnValuesParityTests.cs) — `ReturnValues` variants across `PutItem` (`ALL_OLD`, `NONE`), `UpdateItem` (`ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`), and `DeleteItem` (`ALL_OLD`).
- [`ExpressionValidationOrderParityTests`](../../tests/DynamoDbLite.Parity.Tests/ExpressionValidationOrderParityTests.cs) — raw reserved words in expressions are rejected with `ValidationException` *before* any lookup or mutation, across `DeleteItem`, `Query`, `Scan`, `TransactWriteItems`, `TransactGetItems`, and `BatchGetItem`.
- [`BatchParityTests`](../../tests/DynamoDbLite.Parity.Tests/BatchParityTests.cs) — `BatchGetItem` happy path; `BatchWriteItem` with put + delete in a single batch; `BatchWriteItem` across two tables.
- [`SecondaryIndexParityTests`](../../tests/DynamoDbLite.Parity.Tests/SecondaryIndexParityTests.cs) — GSI query across projection variants: `INCLUDE` returns projected attributes only; `KEYS_ONLY` returns only table + index keys; `ALL` returns every attribute.
- [`LocalSecondaryIndexParityTests`](../../tests/DynamoDbLite.Parity.Tests/LocalSecondaryIndexParityTests.cs) — LSI query with `begins_with` on the alternate sort key; `INCLUDE` projection returns projected attributes only.
- [`ReservedWordParityTests`](../../tests/DynamoDbLite.Parity.Tests/ReservedWordParityTests.cs) — raw reserved words in `UpdateExpression`/`ConditionExpression`/`ProjectionExpression` throw `AmazonDynamoDBException` with `ErrorCode == "ValidationException"`; the same word escaped via `ExpressionAttributeNames` is accepted.
- [`EmptyStringParityTests`](../../tests/DynamoDbLite.Parity.Tests/EmptyStringParityTests.cs) — empty-string scalar values round-trip through `PutItem` + `GetItem` (real DynamoDB rejected these pre-2020; current behavior accepts them).
- [`SizeOperatorParityTests`](../../tests/DynamoDbLite.Parity.Tests/SizeOperatorParityTests.cs) — `size()` in `ConditionExpression` on `UpdateItem` (success and `ConditionalCheckFailedException`); `size()` in `FilterExpression` on `Scan`.

## Uncovered (permanently out of scope)

Each item below is deliberate — there's a load-bearing reason that doesn't go away with more time or effort.

- **Real AWS DynamoDB cloud backend.** Requires credentials, costs money, network-dependent. The three local backends already exercise the contract; a cloud backend would prove the same thing at recurring cost and CI flakiness.
- **TTL parity.** `amazon/dynamodb-local` runs TTL on a long internal cron — expiration windows are minutes-to-hours, which makes CI-friendly cross-backend tests impractical. DynamoDbLite's own TTL behaviour is covered in the main test suite; cross-backend equivalence isn't observable without waiting for the container's cron.
- **Export / Import.** Out of scope per [`docs/adrs/index.md`](../adrs/index.md). The semantics are S3-coupled in real DynamoDB; an in-process emulator and `amazon/dynamodb-local` necessarily diverge from S3, so there's nothing meaningful to assert across the three backends.
- **Cross-client response-shape equality.** Replaced by the explicit-expected-outcome strategy. The three clients legitimately differ on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages; a shared bug between two implementations would also pass cross-comparison silently. Each test asserts what the AWS API contract says should happen, not what each client happens to return.

## Three-backend rationale

Three backends close the file-vs-memory drift surface: the in-memory and file-based stores have different concurrency strategies and have drifted before. The container is the contract reference; the lite backends are the implementations that have to match it.

## Test shape

A `ParityBackend` enum drives one axis of the test theory, mirroring the `StoreType` pattern in the main test project's [`DynamoDbClientFixture`](../../tests/DynamoDbLite.Tests/Fixtures/DynamoDbClientFixture.cs):

```csharp
public enum ParityBackend
{
    DdbLite,        // DynamoDbClient (in-memory SQLite)
    DdbLiteFile,    // DynamoDbClient (file-based SQLite, WAL)
    DynamoDbLocal,  // AmazonDynamoDBClient -> amazon/dynamodb-local container
}
```

The collection fixture at [`DynamoDbFixture`](../../tests/DynamoDbLite.Parity.Tests/Fixtures/DynamoDbFixture.cs) owns one client per backend. Tests are `[Theory]` methods with a single [`[BackendData]`](../../tests/DynamoDbLite.Parity.Tests/Fixtures/BackendDataAttribute.cs) attribute that emits one row per backend tagged with a `Backend` trait, and resolve the client through `fixture.ClientAsync(backend, ct)`. Adding a parity scenario costs one method body and one `[BackendData]` line. The trait is what makes `--filter "Backend=DdbLite"` work — see [Running selectively](#running-selectively) below.

## Container lifecycle

One container for the entire parity-test run, started in `DynamoDbFixture.InitializeAsync`. Container start is the slow step; per-test container start would push the suite into the minute range.

Each test creates its own table with a unique name from `TestTables.UniqueName(prefix)` and leaves it. No per-test teardown — the fixture disposes the three backends at the end of the run, and accumulation across the suite is negligible for SQLite and irrelevant for the container.

`DisposeAsync` order matters: the file-based `DynamoDbClient` disposes before `FileBasedTestHelper.Cleanup` runs, so the SQLite handle is closed before `File.Delete` touches the `.db` file. Skipping that order causes `File.Delete` to throw `IOException` on Windows.

## Assertion strategy

Each test asserts an explicit expected outcome — item count, attribute values, status code, exception type — and runs the same body against each backend. All three must reach the same expectation.

Responses from the three clients are not compared to each other. Real DynamoDB and DynamoDbLite differ legitimately on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages. Stripping those fields would be more code than the tests themselves. A shared bug between implementations would also pass cross-comparison silently.

Tests document the contract. The expected outcome is written from the [AWS DynamoDB API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/), not from whichever implementation happens to be running.

## Error parity

Production code branches on DynamoDB error codes. Tests assert on exception type, not message text:

- `ConditionalCheckFailedException` for failed condition expressions
- `ResourceNotFoundException` for missing tables and indexes
- `ValidationException` for malformed requests
- `TransactionCanceledException` with `CancellationReasons[i].Code` matching the failing item's index

This is the surface where `amazon/dynamodb-local` sometimes diverges from real DynamoDB. Tracking which behavior DynamoDbLite implements is a deliberate decision per scenario, not a default.

## Reserved keywords

DynamoDB's [reserved word list](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) covers ~570 identifiers. Real DynamoDB and `amazon/dynamodb-local` reject unescaped reserved words in expressions. DynamoDbLite matches: each parser checks raw identifiers against [`DynamoDbReservedWords`](../../src/DynamoDbLite/Expressions/DynamoDbReservedWords.cs) and throws `ArgumentException` on a match. Names escaped via `ExpressionAttributeNames` (`#name`) bypass the check, as in real DynamoDB.

## Configuration

Testcontainers reads its endpoint and Ryuk settings from `~/.testcontainers.properties` per developer. The Podman-on-Windows setup needs:

```
docker.host=npipe://./pipe/podman-machine-default
ryuk.disabled=true
```

Docker Desktop users need no file — defaults work. Linux runners (CI) use the native `/var/run/docker.sock` automatically; no env vars needed. Hardcoding the endpoint in C# would break mixed-environment teams; the per-user file is the right knob.

## Running selectively

Every parity test is parameterized through `[BackendData]`, which tags each row with a `Backend` trait. The fixture starts `amazon/dynamodb-local` lazily, so a run that never requests `DynamoDbLocal` never spins up a container.

```
dotnet test tests/DynamoDbLite.Parity.Tests --filter "Backend=DdbLite"           # in-memory only
dotnet test tests/DynamoDbLite.Parity.Tests --filter "Backend=DdbLiteFile"       # file-backed SQLite only
dotnet test tests/DynamoDbLite.Parity.Tests --filter "Backend=DynamoDbLocal"     # real DynamoDB Local
dotnet test tests/DynamoDbLite.Parity.Tests --filter "Backend=DdbLite|Backend=DdbLiteFile"   # both lite backends, no container
```

Wall-clock difference matters for the inner-dev loop — the container is the bulk of the cost, so filtering to lite backends roughly halves run time. CI should always run the full matrix (no filter).
