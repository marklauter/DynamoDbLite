# CLAUDE.md

In-process `IAmazonDynamoDB` backed by SQLite — local dev/testing, mobile apps.

## Commands

- Build: `dotnet build "src/DynamoDbLite.slnx"`
- Test: `dotnet test "src/DynamoDbLite.slnx"`
- Single test: `dotnet test "src/DynamoDbLite.slnx" --filter "FullyQualifiedClassName~MethodName"`
- Format: `dotnet format "src/DynamoDbLite.slnx" --verbosity normal`

## Before Writing Code or Tests

Read `.claude/code-style.md`. Read `.claude/testing.md`.

## Reference

- Architecture: `.claude/architecture.md`
- Tech stack: `.claude/tech-stack.md`
- Gotchas: `.claude/gotchas.md`

## Tech Debt

Read `docs/tech-debt/readme.md` before creating records.
Read `docs/tech-debt/index.md` for status — exclude known items from code review findings.
