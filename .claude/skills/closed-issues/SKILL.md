---
name: closed-issues
description: Lists closed GitHub issues, optionally filtered by label or keyword. Use to check if a problem was already resolved.
---

Lists closed issues using `gh`. Infers label/keyword from context.

```bash
gh issue list --state closed
gh issue list --state closed --label "tech-debt"
gh issue list --state closed --label "tech-debt" --search "keyword"
```

Summarize results concisely.
