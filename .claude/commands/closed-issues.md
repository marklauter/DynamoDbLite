---
name: closed-issues
description: List closed GitHub issues, optionally filtered by label or keyword.
allowed-tools: Bash
user-invocable: true
---

Like `/issues` but hardcoded to `--state closed`. Run `gh` directly — do NOT `cd`.

```bash
gh issue list --state closed
gh issue list --state closed --label "tech-debt"
gh issue list --state closed --label "tech-debt" --search "keyword"
```

Infer label/keyword from context. Summarize results concisely.
