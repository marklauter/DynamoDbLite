---
name: file-issue
description: Files a GitHub issue (tech-debt or bug report) using gh CLI. Use when code review reveals tech debt, bugs, or behavioral mismatches vs. real DynamoDB.
model: haiku
allowed-tools: Bash, Read, Grep, Glob
---

Read the matching template for required fields and allowed values:

- **Tech debt**: `.github/ISSUE_TEMPLATE/tech-debt.yml`
- **Bug report**: `.github/ISSUE_TEMPLATE/bug-report.yml`

Steps:
1. Infer issue type (tech-debt vs bug) from context; if ambiguous, ask.
2. Read the template file. Extract field names, dropdown options, and required/optional flags.
3. Search for duplicates: `gh issue list --label LABEL --search "keywords"`. If a match exists, show it and ask whether to proceed or skip.
4. Infer field values from context; ask only for what's missing.
5. Build the body with `### Field Name` sections matching the template's `id` values. Use `_No response_` for empty optional fields.
6. Show the full issue body for confirmation before creating.
7. Run `gh issue create --template "TEMPLATE.yml" --title "TITLE" --body "BODY"`. Display the returned URL.
