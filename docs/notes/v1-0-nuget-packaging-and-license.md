# v1.0 NuGet packaging and LICENSE

Tags: release, v1.0, packaging
Verify LICENSE at repo root and set NuGet package metadata in DynamoDbLite.csproj before publishing v1.0.


## Observation

State of the .csproj packaging properties and the repo-root LICENSE has not been verified during this work. Both are first-impression items on the NuGet listing.

## Interpretation

These are cheap to set right before publish and awkward to hot-fix after — a listing without a license tag, project URL, or description looks abandoned and reduces install confidence. Adding them after the first push means a second package version just for metadata.

## Next

- Verify `LICENSE` (or `LICENSE.md`) is at the repo root. If missing, add — likely MIT given the library style.
- In `src/DynamoDbLite/DynamoDbLite.csproj`, set:
  - `PackageId` (probably `DynamoDbLite`)
  - `Title`, `Description`, `Authors`, `Company` (if applicable)
  - `PackageTags` (e.g. `dynamodb;sqlite;testing;mock;in-memory`)
  - `RepositoryUrl`, `RepositoryType=git`, `ProjectUrl`
  - `PackageLicenseExpression` (e.g. `MIT`)
  - Optional: `PackageIcon`, `PackageReadmeFile`
- Build with `dotnet pack` locally and inspect the produced `.nupkg` with `dotnet nuget package` or unzip to confirm metadata.
