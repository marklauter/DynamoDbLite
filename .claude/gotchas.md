# Gotchas

## SQLite
- Dapper column aliases needed: snake_case SQL columns → PascalCase C# properties

## AWS SDK v4
- `BOOL` and `NULL` on `AttributeValue` are `bool?`, not `bool`; `IsBOOLSet`/`IsSSSet` etc. may not exist — use null checks
- `request.Limit` is `int?`, not `int`

## Analyzers
- IDE0058: `HashSet.Add`, `List.Remove`, etc. return values need `_ =` discard
- CA1707: suppress in test project (underscore test names)
- CA1305: `DateTime.Parse` needs `CultureInfo.InvariantCulture`
- IDISP004: false positive on `JsonElement.ArrayEnumerator` foreach — use pragma
- IDISP016/017: pragma must go before the `var` declaration line, not the usage line

## Superpower
- `Parse.Ref`/`Parse.Chain` conflict with local `Parse` methods — use `Superpower.Parse.Ref`/`Superpower.Parse.Chain`
- Tokenizer: use `new` keyword when hiding inherited `SkipWhiteSpace` method

## Windows / Build
- Use `rm` not `del` in bash
- Use quoted paths for dotnet commands: `dotnet build "D:\..."`
