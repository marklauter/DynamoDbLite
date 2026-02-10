# Testing

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
