# v1.0 public API surface audit

Walk-through document. Each finding has a **Decision** line — fill it in (`✅ fix in this pass` / `❌ keep` / `🟡 defer to v1.1`) as we work through them.

Scope: every public type and member in the top-level `DynamoDbLite` namespace. Internal namespaces (`SqliteStores`, `SqliteStores.Models`, `Expressions`) are enforced by ArchUnit and were verified still clean during the sweep.

## Findings

Ordered roughly by reversibility — irreversible / breaking-change-cost-after-v1.0 first.

---

### F1. `Paginators` returns `null` silently — **TRIAGED**

- **Status** — Tracked as a standalone note: [paginators-property-returns-null-silently](notes/paginators-property-returns-null-silently.md). Decision (implement / throw / document) deferred to that note; remove from this review's open list.

---

### F2. `CreateDefaultClientConfig` static factory — **DONE**

- **Resolution** — Deleted from `src/DynamoDbLite/DynamoDbClient.cs`. Zero callers in repo. Build + 891 tests green after removal.

---

### F3. `CreateDefaultServiceClient` static factory — **DONE**

- **Resolution** — Deleted in the same edit as F2. The method only threw `NotSupportedException` — was never a creation path. The two real paths (`new DynamoDbClient(options)` and `services.AddDynamoDbLite(...)`) are unaffected.

---

### F4. Exception ctor nullability stricter than BCL convention — **REJECTED (false positive)**

- **Resolution** — Keep non-null. Rationale: if a caller wants a null-message exception they can use the parameterless `DynamoDbLiteConfigurationException()` ctor; passing `null` explicitly to `(string message)` is never the intent. Forcing non-null on the message-carrying overloads is the correct contract for this exception type.

---

### F5. Deprecated `UpdateItemAsync` overloads — no `[Obsolete]` — **WON'T FIX (match interface)**

- **Resolution** — Verified via reflection that `IAmazonDynamoDB` itself carries no `[Obsolete]` on any of these legacy method overloads (only on the underlying request *fields*). Adding `[Obsolete]` to DynamoDbLite's overloads would make it strictly more opinionated than the interface it implements. Keep current shape; signature parity wins.

---

### F6. `ValidateConnectionString` return value is unusual — **REJECTED (false positive)**

- **Resolution** — Validate-and-return is a standard functional pattern; same shape as `ArgumentNullException.ThrowIfNull(x)` returning the validated value for expression-position use. The reviewer characterized it as unusual relative to procedural `Validate*` → `void`, but the FP form is idiomatic .NET and well-precedented in the BCL. Keep as-is.

---

### F7. `ValidateConnectionString` doesn't reveal normalization — **REJECTED (false positive)**

- **Resolution** — The method validates, it does not normalize — and there's no reason a consumer would expect otherwise. It's the builder's validator exposed for reuse; the internal connection-string mutations belong to the store, not to the validator. No doc change needed.

---

### F8. ArchUnit rule omits `DynamoDbLite.Serialization`

- **File:line** — `tests/DynamoDbLite.Tests/Architecture/ArchitectureTests.cs:89`
- **Member** — `InternalNamespacesContainOnlyInternalTypes` regex: `^DynamoDbLite\.(SqliteStores|SqliteStores\.Models|Expressions)$`
- **Concern** — `DynamoDbLite.Serialization` has 9 wire-record/extension types — all currently `internal sealed`, all clearly internal-by-intent — but the ArchUnit guardrail doesn't cover them. If someone adds `public` accidentally, the build won't catch it.
- **Recommendation** — Extend regex to `…|Serialization)$`, update `.Because(...)` to mention wire DTOs. Trivial, no risk.
- **Severity** — Low (process gap, not current leak). But worth fixing in this pass because it's two characters.
- **Decision** —

---

### F9. Missing XML docs on `DynamoDbLiteConfigurationException`

- **File:line** — `src/DynamoDbLite/DynamoDbLiteConfigurationException.cs:3-18`
- **Members** — Type + all three ctors.
- **Concern** — Every other public type has rich XML docs. Exception type has none — blank IntelliSense for the exception consumers will catch. Wiki prose already exists (`DI-and-Configuration.md:75-77`).
- **Recommendation** — Add `<summary>` on type ("thrown by `WithConnectionString`, `ValidateConnectionString`, `Build`") + standard ctor docs.
- **Severity** — Low. Docs polish.
- **Decision** —

---

### F10. Missing XML docs on `ServiceCollectionExtensions`

- **File:line** — `src/DynamoDbLite/ServiceCollectionExtensions.cs:9-25`
- **Members** — Class + `AddDynamoDbLite`.
- **Concern** — Registration entry point — first symbol most consumers touch — has no `<summary>`. Wiki covers eager validation, `TryAddSingleton`, singleton-`IAmazonDynamoDB` registration; none surfaces in IntelliSense.
- **Recommendation** — `<summary>` covering: registers `IAmazonDynamoDB` as singleton, `TryAddSingleton` (won't overwrite), eager validation, throws `DynamoDbLiteConfigurationException`. `<exception>` tag for the throw. `<param>` for `configure`.
- **Severity** — Low. Docs polish.
- **Decision** —

---

## Verified clean (no action expected)

- **`DynamoDbClient` constructor** (`DynamoDbClient.cs:12`) — entry point, sensibly public.
- **`Config` property** (`DynamoDbClient.cs:27`) — required by `IAmazonService`. Optional micro-optimization: `Lazy<AmazonDynamoDBConfig>`. Not correctness.
- **`DetermineServiceOperationEndpoint`** (`DynamoDbClient.Admin.cs:227`) — required; returns fake `http://dynamodb.localhost`. Consider documenting the placeholder.
- **All operation methods** across the 11 `DynamoDbClient.*.cs` partials — required by `IAmazonDynamoDB`. Async + legacy sync convenience overloads, stub `NotImplementedException`s, and `NotSupportedException` throws for PartiQL all conform to the interface.
- **`DynamoDbLiteOptions`** — sealed positional record, single required non-null parameter, well-documented.
- **`DynamoDbLiteOptionsBuilder`** — sealed, fluent `WithConnectionString` returns `this`; `Build()` is correctly `internal`.
- **`DynamoDbLiteConfigurationException`** — sealed, three-ctor pattern, correctly omits obsolete `SerializationInfo` ctor (deprecated on .NET 8+).
- **`ServiceCollectionExtensions.AddDynamoDbLite`** — idiomatic; `TryAddSingleton`; eager validation; nullable-checks `configure`. (`static` class is implicitly sealed; the audit re-check is satisfied.)
- **Top-level utility files** — `AttributeValueSerializer`, `KeyConditionSqlBuilder`, `KeyHelper`, `TtlEpochParser` all `internal static`.
- **Sub-namespaces** — `SqliteStores`, `SqliteStores.Models`, `Expressions`, `Serialization` — no `public` types anywhere. ArchUnit covers the first three (Serialization is the gap in F8).
- **No namespace leaks** — no file under any subdirectory declares the bare `namespace DynamoDbLite;`.

## Open questions to resolve during walk-through

1. **`[Obsolete]` policy (F5)** — only on legacy `UpdateItemAsync`, or also on legacy `Query`/`Scan` legacy-condition overloads? Or none (strict SDK parity)?
2. **`Action<DynamoDbLiteOptions>` overload** — should `AddDynamoDbLite` also accept a pre-built options instance? Non-breaking to add post-v1.0; flag is whether to ship both shapes in 1.0.
3. **`KeyConditionSqlBuilder` placement** — sits in top-level `DynamoDbLite` namespace. Internal so public surface is unaffected, but cosmetically it belongs in `Expressions` or `SqliteStores`. Move now or leave?

## Process notes

- All three sub-audits done by Opus agents in parallel; merged here. Original agent outputs not preserved separately.
- ArchUnit boundary check confirmed clean for `SqliteStores`, `SqliteStores.Models`, `Expressions`. Gap is `Serialization` (F8).
- Total public members reviewed: ~95 across the `DynamoDbClient` partial + the 4 config/DI/exception files. 10 findings — 1 triaged out to its own note (F1), 3 resolved (F2, F3, F5 won't-fix), 3 rejected as false positives (F4, F6, F7), 3 low open. No "must-not-ship" blockers.
