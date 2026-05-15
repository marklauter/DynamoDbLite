# v1.0 CI gate on push and PR

Tags: release, v1.0, ci
Add GitHub Actions workflow running dotnet format, build, main suite, and parity suite on every push and PR.


## Observation

All gating is local — `dotnet test` runs on developer machines. No automation prevents a broken commit from landing on `main`.

## Interpretation

Today the team is small enough that local discipline holds. That doesn't last as soon as contributors arrive or a fast-moving day produces a tired push. CI is the durable signal — it's also what the package consumers will look at (the green badge on the README).

## Next

Add `.github/workflows/ci.yml`. Required steps:

- `actions/checkout@v4`
- `actions/setup-dotnet@v4` with the .NET version this repo targets (net10.0).
- `dotnet format src/DynamoDbLite.slnx --severity info --verify-no-changes`
- `dotnet build src/DynamoDbLite.slnx --no-restore`
- `dotnet test src/DynamoDbLite.Tests --no-build`
- `dotnet test src/DynamoDbLite.Parity.Tests --no-build`

Triggers: `push` to `main`, `pull_request` to `main`. Branch protection rule on `main` requiring CI green before merge.

Parity tests use Testcontainers for `amazon/dynamodb-local`. On Linux runners (the GitHub-hosted default) Docker is available out of the box — no extra setup. Verify the container starts and the full 180-test parity suite passes once on the runner before relying on it.
