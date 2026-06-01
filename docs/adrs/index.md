---
title: Architecture Decisions
summary: Index of architectural decision records — the front door to the ADR set.
document:
  tags: [adr, index]
---

# Architecture Decisions

Index of architectural decision records. Each ADR captures a single decision; this file is the front door.

| # | Title | Status |
|---|---|---|
| [0001](0001-storage-model.md) | Storage Model | Accepted |
| [0002](0002-sqlite-lifetime.md) | SQLite Lifetime | Accepted |
| [0003](0003-concurrency-strategy.md) | Concurrency Strategy | Superseded by [0008](0008-in-memory-needs-no-app-lock.md) |
| [0004](0004-behavioral-fidelity.md) | Behavioral Fidelity | Accepted |
| [0005](0005-implementation-phases.md) | Implementation Phases | Informational |
| [0006](0006-out-of-scope-operations.md) | Out-of-scope Operations | Superseded by [0007](0007-not-supported-exception-for-out-of-scope.md) |
| [0007](0007-not-supported-exception-for-out-of-scope.md) | `NotSupportedException` for Out-of-scope Operations | Accepted |
| [0008](0008-in-memory-needs-no-app-lock.md) | In-memory store needs no in-process lock | Accepted |

New architectural decisions go here as new numbered ADRs. Don't edit a shipped ADR's intent — supersede it with a new one and mark the old as Superseded.
