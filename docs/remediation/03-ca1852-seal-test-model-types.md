---
title: CA1852 — test model types can be sealed
summary: Seven test-only model classes are unsealed with no subtypes; seal them. CA1852 is a warning-level rule set in every house .editorconfig.
tags: [todo, remediation, ddblite, analyzers, tests, ca1852]
created: 2026-07-16
priority: high
effort: low
status: open
---

CA1852 ("type can be sealed") fires on seven test-fixture model classes that have
no subtypes and aren't externally visible. The rule is set to `warning` in every
house `.editorconfig` (plumber, pool, lexi, ddblite all share `.editorconfig:381`);
plumber passes because its types are already sealed.

## Fix

Real fix, not suppression — seal each type:

```csharp
internal sealed class SimpleItem { ... }
```

These are DTO-shaped test items; sealing has no behavioral effect and matches the
house convention (sealed concretes).

## Blocking sites (7)

- tests/DynamoDbLite.Tests/Models/CollectionItem.cs:6
- tests/DynamoDbLite.Tests/Models/CompositeKeyItem.cs:6
- tests/DynamoDbLite.Tests/Models/EnumItem.cs:13
- tests/DynamoDbLite.Tests/Models/GsiItem.cs:6
- tests/DynamoDbLite.Tests/Models/NumericKeyItem.cs:6
- tests/DynamoDbLite.Tests/Models/SimpleItem.cs:6
- tests/DynamoDbLite.Tests/Models/VersionedItem.cs:6
