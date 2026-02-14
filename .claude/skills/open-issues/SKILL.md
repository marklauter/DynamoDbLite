---
name: open-issues
description: Lists open GitHub issues, optionally filtered by label or keyword. Use before flagging code review findings to check if an issue already exists.
model: haiku
allowed-tools: Bash
---

Lists open issues using `gh`. Infers label/keyword from context.

```bash
gh issue list --state open
gh issue list --state open --label "tech-debt"
gh issue list --state open --label "tech-debt" --search "keyword"
```

Summarize results concisely.
