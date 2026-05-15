# Parity Tests

Design for the integration tests that prove DynamoDbLite matches real DynamoDB behavior. Tests run the same scenario against `DynamoDbClient` (SQLite-backed, in-proc) and `AmazonDynamoDBClient` pointed at `amazon/dynamodb-local` (Testcontainers + Podman). Tracks Phase 14 in [`architecture-decisions.md`](architecture-decisions.md).

Scaffolding exists today: [`DynamoDbFixture`](../src/DynamoDbLite.Tests/Fixtures/DynamoDbFixture.cs) starts the container, [`DynamoDbFixtureCollection`](../src/DynamoDbLite.Tests/Fixtures/DynamoDbFixtureCollection.cs) defines the xUnit collection, and [`TestcontainersSmokeTest.cs`](../src/DynamoDbLite.Tests/TestcontainersSmokeTest.cs) confirms the wiring end-to-end. No parity scenarios are written yet.

## Shape

A `Backend` enum drives one axis of the test theory, mirroring the existing `StoreType` pattern in [`DynamoDbClientFixture`](../src/DynamoDbLite.Tests/Fixtures/DynamoDbClientFixture.cs):

```csharp
public enum Backend
{
    Lite,        // DynamoDbClient (in-memory SQLite)
    LiteFile,    // DynamoDbClient (file-based SQLite, WAL)
    Real         // AmazonDynamoDBClient → amazon/dynamodb-local
}
```

A collection fixture owns one client per backend. Tests are `[Theory]` methods with one `[InlineData(Backend.X)]` per backend, and resolve the client through `fixture.Client(backend)`. Adding a parity test costs one method body and three `InlineData` lines.

Including both `Lite` and `LiteFile` closes a real bug surface — the in-memory and file-based stores have different concurrency strategies and have drifted before.

## Container Lifecycle

One container for the entire parity-test run, started in the collection fixture's `InitializeAsync`. Container start is the slow step; per-test container start would push the suite into the minute range.

Each test creates its own table with a `Guid.NewGuid()`-suffixed name and deletes it in teardown. Tables are cheap (sub-second create against DynamoDB Local), and per-test isolation removes ordering hazards. The single `TestTable` created in the current `DynamoDbFixture.InitializeAsync` is for the smoke test only — parity tests do not depend on it.

## Assertion Strategy

Each test asserts an explicit expected outcome — item count, attribute values, status code, exception type, error code — and runs the same body against each backend. Both must reach the same expectation.

Do not compare responses from the two clients to each other. Real DynamoDB and DynamoDbLite differ legitimately on `TableArn`, `CreationDateTime`, `ResponseMetadata.RequestId`, capacity numbers, and free-text error messages. Stripping those fields field-by-field is more code than the tests themselves. Worse, a shared bug between the two implementations would pass a cross-comparison silently.

Tests document the contract. The expected outcome is written from the [AWS DynamoDB API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/), not from whichever implementation happens to be running.

## Error Parity

Production code branches on DynamoDB error codes. Assert on exception type and `ErrorCode` string, not message text:

- `ConditionalCheckFailedException` for failed condition expressions
- `ResourceNotFoundException` for missing tables and indexes
- `ValidationException` for malformed requests
- `TransactionCanceledException` with `CancellationReasons[i].Code` matching the failing item's index

This is the surface where DynamoDB Local sometimes diverges from real DynamoDB. Tracking which behavior DynamoDbLite implements is a deliberate decision per scenario, not a default.

## Project Layout

Parity tests live in a new project: `src/DynamoDbLite.Parity.Tests/`. The existing `DynamoDbLite.Tests` project stays container-free for fast local iteration.

The split signals that a container runtime is required, lets `dotnet test src/DynamoDbLite.Tests` stay sub-second, and gives CI a separate job with its own timeout and matrix. The project picks up the standard test setup from [`src/Directory.Build.props`](../src/Directory.Build.props) by virtue of the `*.Tests` name suffix.

The first-run scope sets the pattern for everything that follows — invest in the harness shape and the first three scenarios before expanding.

## Initial Coverage

Map tests one-to-one against the parity claims in `README.md`. Target for the first pass:

- `PutItem` + `GetItem` round-trip across each attribute type — S, N, B, BOOL, NULL, L, M, SS, NS, BS
- `ConditionExpression` paths: `attribute_exists`, `attribute_not_exists`, `begins_with`, and the `ConditionalCheckFailedException` failure case
- `Query` with `KeyConditionExpression`, `FilterExpression`, `ScanIndexForward`, and `LastEvaluatedKey` pagination
- `UpdateItem` per operator: `SET`, `ADD`, `REMOVE`, `list_append`, `if_not_exists`
- `TransactWriteItems` atomicity — one failing condition rolls back the batch with `CancellationReasons` populated
- `BatchGetItem` unprocessed-keys behavior at the size limit
- GSI query against each projection type — `KEYS_ONLY`, `INCLUDE`, `ALL`

Each scenario maps to a known production failure mode. Expanding past this list is cheap once the harness is proven.

## Configuration

Testcontainers reads its endpoint and Ryuk settings from `~/.testcontainers.properties` per developer. The Podman setup needs:

```
docker.host=npipe://./pipe/podman-machine-default
ryuk.disabled=true
```

Docker Desktop users need no file — defaults work. CI sets the env vars in the workflow rather than checking in a file. Hardcoding the endpoint in C# would break mixed-environment teams; the per-user file is the right knob.

## Open Decisions

- Two backends (`Lite`, `Real`) or three (`Lite`, `LiteFile`, `Real`). Three closes the file-vs-memory drift surface; two keeps the matrix small.
- Initial scope — the seven scenarios above, or a different list aligned to specific README claims.
- CI strategy — separate job, scheduled run, or every-PR.
