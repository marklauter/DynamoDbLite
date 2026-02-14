# CLAUDE.md

In-process `IAmazonDynamoDB` backed by SQLite — local dev/testing, mobile apps.

## Commands

- Build: `dotnet build "src/DynamoDbLite.slnx"`
- Test: `dotnet test "src/DynamoDbLite.slnx"`
- Single test: `dotnet test "src/DynamoDbLite.slnx" --filter "FullyQualifiedClassName~MethodName"`
- Format: `dotnet format "src/DynamoDbLite.slnx" --verbosity normal`

## Principles

DRY — all authored artifacts (code, skills, configs, docs). One source of truth; reference, don't duplicate.

## Before Writing Code or Tests

Read `.claude/code/code-style.md`. Read `.claude/code/testing.md`.

## Reference

- Architecture: `.claude/code/architecture.md`
- Tech stack: `.claude/code/tech-stack.md`
- Gotchas: `.claude/code/gotchas.md`

## Tech Debt & Issues

Issue tracking on GitHub via `gh` CLI. Check existing issues before flagging code review findings.
