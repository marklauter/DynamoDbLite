# Testing

## Philosophy

**Don't test what you don't own.** Tests should exercise DynamoDbLite's behavior, not the AWS SDK or SQLite. Don't write tests that merely assert AWS SDK serialization, Dapper mapping, or SQLite SQL behavior in isolation — those are the responsibility of their respective maintainers. Focus tests on the contract DynamoDbLite exposes: given a DynamoDB API call, does our implementation return the correct response?

**Test the contract, not the construction.** Assert on what a method promises to do (its public contract), not how it does it internally. Don't assert on private state, internal method calls, or SQL queries — assert on the DynamoDB response. Refactoring internals should never break tests.

**Tests are documentation.** A well-named test suite is a living spec. Someone unfamiliar with the codebase should be able to read test names and understand what the system does and what edge cases it handles.

## xUnit v3

Uses **xUnit v3** (not the deprecated v2). When creating test projects:

- Reference `xunit.v3` (not `xunit`) — this single package includes assertions, core, and runner
- Reference `xunit.runner.visualstudio` v3.x with `<PrivateAssets>all</PrivateAssets>`
- Reference `Microsoft.NET.Test.Sdk`
- Add a global using: `<Using Include="Xunit" />`
- Set `<IsPackable>false</IsPackable>`
- Do **not** reference legacy `xunit`, `xunit.core`, or `xunit.assert` packages

Example test csproj PackageReferences:
```xml
<PackageReference Include="Microsoft.NET.Test.Sdk" Version="18.0.1" />
<PackageReference Include="xunit.runner.visualstudio" Version="3.1.5">
  <PrivateAssets>all</PrivateAssets>
  <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
</PackageReference>
<PackageReference Include="xunit.v3" Version="3.2.2" />
```

Run tests with `dotnet test` from the solution or project directory.
