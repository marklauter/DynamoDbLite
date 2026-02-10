# Allocation & Performance Optimization Opportunities

## High-Impact

### 1. MemoryStream `.ToArray()` — Avoidable Copies

Multiple files call `.ToArray()` on `MemoryStream`, allocating a full copy of the buffer each time. This is especially costly in hot paths like condition evaluation and binary comparisons.

**Files affected:**
- `ConditionExpressionEvaluator.cs` — `CompareBytes(left.B.ToArray(), right.B.ToArray())`
- `UpdateExpressionEvaluator.cs` — `.BS.Any(eb => eb.ToArray().AsSpan().SequenceEqual(...))`
- `AttributeValueSerializer.cs` — `writer.WriteBase64StringValue(value.B.ToArray())`
- `KeyHelper.cs` — `Convert.ToBase64String(value.B.ToArray())`

**Fix:** Use `TryGetBuffer(out ArraySegment<byte> buffer)` to get a span over the existing buffer without copying.

### 2. SQL String Concatenation in Query Hot Paths

`SqliteStore.cs` builds SQL queries using `+=` on strings, which allocates intermediate strings on every query:

```csharp
sql += $" AND {skWhereSql}";
sql += $" ORDER BY {orderByColumn} {direction}";
```

**Fix:** Use `StringBuilder` or `DefaultInterpolatedStringHandler` for dynamic SQL assembly.

### 3. LINQ to Loop Replacements (Validation Paths)

`DynamoDbClient.TableManagement.cs` enumerates key schemas 3 times with separate LINQ queries:

```csharp
var hashKeys = keySchema.Where(...).ToList();
var rangeKeys = keySchema.Where(...).ToList();
var keyAttributeNames = keySchema.Select(...).ToHashSet();
```

**Fix:** Single-pass `foreach` loop classifying into hash/range/names simultaneously.

### 4. `IndexTableName` String Allocation

`SqliteStore.IndexTableName` is called on every index read/write and allocates via interpolation:

```csharp
$"idx_{tableName}_{indexName}"
```

**Fix:** Use `string.Create(length, (tableName, indexName), ...)` with exact pre-computed length, or cache results.

### 5. List/Dictionary Pre-sizing

Throughout the codebase, collections are created without capacity hints:

- JSON deserialization loops (`DeserializeKeySchema`, `DeserializeAttributeDefinitions`, `DeserializeIndexDefinitions`) — use `GetArrayLength()` to pre-allocate
- `HashSet<(string,string,string)>` in transactions — pass `actions.Count` as capacity
- `Dictionary<string, string>` in legacy scan conversion — pass `conditions.Count`

### 6. `IncrementPrefix` Char Array

`KeyConditionSqlBuilder.cs` allocates a `char[]` via `ToCharArray()`:

```csharp
var chars = prefix.ToCharArray();
chars[^1]++;
return new string(chars);
```

**Fix:** `string.Create(prefix.Length, prefix, (span, p) => { p.AsSpan().CopyTo(span); span[^1]++; })`

## Medium-Impact

### 7. Binary Set Operations — Repeated `.ToArray()` in LINQ Predicates

`UpdateExpressionEvaluator.cs` calls `.ToArray()` inside `Any`/`RemoveAll` lambdas, meaning every element comparison allocates:

```csharp
existing.BS.Any(eb => eb.ToArray().AsSpan().SequenceEqual(b.ToArray()))
```

**Fix:** Extract `b.ToArray()` (or better, `TryGetBuffer`) outside the lambda, compare using spans.

### 8. LINQ `.Select().ToList()` for Serialization

`SqliteStore.cs` creates anonymous objects via LINQ for JSON serialization:

```csharp
JsonSerializer.Serialize(keySchema.Select(k => new { ... }).ToList())
```

**Fix:** Use a `foreach` loop with pre-allocated `List<T>` and a small named struct/record.

### 9. Dapper `.AsList()` Calls

Multiple query results call `.AsList()` which may copy if the underlying type isn't already a `List<T>`. Dapper's `QueryAsync` already returns a collection that supports `AsList()` without copying in most cases, but this should be verified.

## Lower-Impact / Future Considerations

- **ValueTask** for `GetTtlAttributeNameAsync`, `GetKeySchemaAsync` — configuration lookups that could benefit from synchronous fast paths
- **ArrayPool<T>** for temporary `byte[]` buffers in binary comparisons
- **Integer boxing** in string interpolation (`$"#legacyN{i}"`) — minor, but `string.Create` avoids it
- **Record to struct** for high-frequency internal types like `ItemRow`, `IndexItemRow` (measure first — Dapper materialization matters here)
