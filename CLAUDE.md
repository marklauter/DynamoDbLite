# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

In-process `IAmazonDynamoDB` backed by `SQLite` for local dev/testing, or mobile apps.

## Quick Reference

- Solution file: `src/DynamoDbLite.slnx`
- Build: `dotnet build "src/DynamoDbLite.slnx"`
- Test: `dotnet test "src/DynamoDbLite.slnx"`
- Single test: `dotnet test "src/DynamoDbLite.slnx" --filter "FullyQualifiedClassName~MethodName"`
- Format: `dotnet format "src/DynamoDbLite.slnx" --verbosity normal`

## Code Style

**Always** Read `.claude/code-style.md` before writing any code.

## Testing

**Always** Read `.claude/testing.md` before creating test projects and before writing tests.

## Architecture

Read `.claude/architecture.md` for component layout and file responsibilities.

## Tech Stack

Read `.claude/tech-stack.md` for platform and dependency info.

## Tech Debt

Read `docs/tech-debt/readme.md` for tech-debt template before creating new tech-debt records.
Read `docs/tech-debt/index.md` for tech-debt record listing and status, and before code reviews and exclude known tech-debt items from review findings.

## Gotchas

Read `.claude/gotchas.md` before debugging build or runtime issues.
