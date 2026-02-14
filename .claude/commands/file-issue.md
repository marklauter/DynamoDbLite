---
name: file-issue
description: File a GitHub issue (tech-debt or bug report) using gh CLI.
allowed-tools: Bash, AskUserQuestion, Read, Grep, Glob
user-invocable: true
---

File a GitHub issue. Before creating, search for duplicates with `gh issue list --label LABEL --search "keywords"`. If a match exists, show it and ask whether to proceed or skip. Infer type (tech-debt vs bug) and fields from context; ask only for what's missing. Show full body for confirmation before running `gh issue create`. Display the returned URL.

IMPORTANT: Run all `gh` commands directly — do NOT `cd` first. The working directory is already the repo root.

## Tech Debt — `tech-debt.yml`

Enums — **area**: Core | SqliteStore | DynamoDbClient | Expressions | KeyConditionSqlBuilder | Serialization | Indexes | Transactions | TTL | Export/Import | Tags | Testing | Other. **type**: Bug | Performance | Observability | Validation | Cleanup | Feature Gap | Safety | API Fidelity | Testing. **priority**: High | Medium | Low.

```bash
gh issue create --template "tech-debt.yml" --title "TITLE" --body "$(cat <<'EOF'
### Area
AREA
### Type
TYPE — description
### Priority
PRIORITY
### Problem
PROBLEM
### Suggested Fix
FIX
### Code References
_No response_
### Notes
_No response_
EOF
)"
```

## Bug Report — `bug-report.yml`

**store-type**: Both | InMemorySqliteStore | FileSqliteStore.

```bash
gh issue create --template "bug-report.yml" --title "TITLE" --body "$(cat <<'EOF'
### Description
DESCRIPTION
### Steps to Reproduce
```csharp
REPRO
```
### Expected Behavior
EXPECTED
### Actual Behavior
ACTUAL
### Store Type
STORE_TYPE
### Notes
_No response_
EOF
)"
```
