# v1.0 public API surface audit

Tags: release, v1.0, api-stability
Audit every public type and member in DynamoDbLite before tagging v1.0. ArchUnit handles internal namespaces; this is for the top-level public surface.


## Observation

The architecture tests in [`ArchitectureTests.cs`](../../src/DynamoDbLite.Tests/Architecture/ArchitectureTests.cs) already enforce that types in `DynamoDbLite.SqliteStores`, `DynamoDbLite.SqliteStores.Models`, and `DynamoDbLite.Expressions` are not public. That removes one whole class of leak.

What ArchUnit does *not* check: whether the public types living in the top-level `DynamoDbLite` namespace are the right public surface. `DynamoDbClient`, `DynamoDbLiteOptions`, `DynamoDbLiteOptionsBuilder`, `DynamoDbLiteConfigurationException`, `ServiceCollectionExtensions` — these are all public by intent. The audit is about the *members* on those types and any other public types we may not have considered.

## Interpretation

Once v1.0 ships, the public surface is the support contract — breaking changes get expensive (semver-major bump, customer-visible migration). The cost of a careful pass now is a few hours; the cost of finding a leaked internal in v1.1 is much more.

## Next

Walk every type in the `DynamoDbLite` namespace (top-level only — ArchUnit covers the rest):

- For each `public` type, ask: does a consumer call this directly, or is it accidental exposure?
- For each public member, ask: does this need to be in the contract, or could it be `internal` / `private`?
- Confirm sealing of concrete public classes (ArchUnit already enforces sealing for non-abstract classes, so this is mostly a re-check).
- Verify nullable annotations on public members are intentional.

Tag the result in CHANGELOG (see [v1-0-changelog-and-release-notes](v1-0-changelog-and-release-notes.md)).
