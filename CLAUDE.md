# CLAUDE.md

In-process `IAmazonDynamoDB` backed by SQLite — local dev/testing, mobile apps.

## Before writing code or tests

- C# guidance (commands, style): [`docs/agents/csharp-guidance.md`](docs/agents/csharp-guidance.md)
- Architecture: [`docs/agents/architecture.md`](docs/agents/architecture.md)
- Testing: [`docs/agents/testing.md`](docs/agents/testing.md)
- Gotchas: [`docs/agents/gotchas.md`](docs/agents/gotchas.md)
- Design rationale: [`docs/adrs/index.md`](docs/adrs/index.md) — architectural decisions; supersede in place, do not edit shipped intent
- Open questions and discoveries: [`docs/notes/`](docs/notes/) — atomic wiki-style notes; one topic per file, latest state only (git keeps history)

## Never

- never add .ConfigureAwait to my code
