# Gotchas

## SQLite / Dapper

Column aliases required: snake_case SQL columns → PascalCase C# properties.

## AWS SDK v4

- `BOOL` and `NULL` on `AttributeValue` are `bool?`, not `bool`. `IsBOOLSet` / `IsSSSet` / etc. may not exist — use null checks.
- `request.Limit` is `int?`, not `int`.

## Analyzers

- `IDE0058`: `HashSet.Add`, `List.Remove`, etc. return values need `_ =` discard.
- `CA1305`: `DateTime.Parse` needs `CultureInfo.InvariantCulture`.
- `IDISP004`: false positive on `JsonElement.ArrayEnumerator` foreach — suppress with pragma.
- `IDISP016` / `IDISP017`: pragma must go before the `var` declaration line, not the usage line.

## Superpower

- `Parse.Ref` / `Parse.Chain` collide with local `Parse` methods — fully qualify as `Superpower.Parse.Ref` / `Superpower.Parse.Chain`.
- Tokenizer: use `new` keyword when hiding the inherited `SkipWhiteSpace`.
