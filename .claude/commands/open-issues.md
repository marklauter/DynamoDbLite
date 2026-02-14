---
name: open-issues
description: List open GitHub issues, optionally filtered by label or keyword.
model: haiku
allowed-tools: Bash
user-invocable: true
---

Like `/issues` but hardcoded to `--state open`. Run `gh` directly — do NOT `cd`.

```bash
gh issue list --state open
gh issue list --state open --label "tech-debt"
gh issue list --state open --label "tech-debt" --search "keyword"
```

Infer label/keyword from context. Summarize results concisely.
