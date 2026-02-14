---
name: issues
description: Search and list GitHub issues (tech-debt, bugs, or all).
allowed-tools: Bash
user-invocable: true
---

Query GitHub issues. Infer label and search terms from context. Default to open issues.

IMPORTANT: Run all `gh` commands directly — do NOT `cd` first. The working directory is already the repo root.

```bash
# All open issues
gh issue list --state open

# By label
gh issue list --label "tech-debt" --state open
gh issue list --label "bug" --state open

# Search by keyword
gh issue list --label "tech-debt" --search "keyword"

# Full details
gh issue view NUMBER
```

If the user provides keywords, search. If they ask about a specific number, view it. Otherwise list all open issues. Summarize results concisely.
