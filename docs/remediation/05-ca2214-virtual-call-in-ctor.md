---
title: CA2214 — overridable method called from constructor
summary: DynamoDbContextFactory's constructor calls an overridable method; a subclass override would run against a half-initialized instance. Restructure so construction calls nothing virtual.
tags: [todo, remediation, ddblite, analyzers, tests, ca2214]
created: 2026-07-16
priority: high
effort: low
status: open
---

CA2214 ("do not call overridable methods in constructors") fires once, in a test
fixture factory. A virtual call during construction dispatches to a subclass
override before the subclass constructor has run — a genuine latent bug, not
ceremony.

## Fix

Inspect `DynamoDbContextFactory` at the flagged line and choose the correct
remedy:

- if the called member has no reason to be overridable, seal it (`private` or
  non-virtual), or
- move the call out of the constructor into an explicit initialization step the
  caller invokes after construction, or
- if a subclass genuinely must customize and the timing is provably safe,
  suppress with a `Justification` naming why (last resort).

Do not blanket-suppress — decide per the actual design.

## Blocking site (1)

- tests/DynamoDbLite.Tests/Fixtures/DynamoDbContextFactory.cs:14
