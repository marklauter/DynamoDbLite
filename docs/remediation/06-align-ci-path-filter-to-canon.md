---
title: Align dotnet.tests.yml path filter to the plumber canon
summary: The test workflow's path filter omits .editorconfig, Directory.Build.targets, global.json, and .gitattributes, so #98's .editorconfig change never ran format/test — which is why main went silently red. This is the root cause.
tags: [todo, remediation, ddblite, ci, root-cause]
created: 2026-07-16
priority: high
effort: low
status: open
---

**Root cause of the red main.** `dotnet.tests.yml` (which hosts both the `format`
and `test` jobs) triggers on:

```yaml
paths: [ "src/**", "tests/**", "Directory.Build.props", "Directory.Packages.props", ".github/workflows/**" ]
```

`.editorconfig` is not in the list. PR #98 changed only `.editorconfig` — raising
CA1307/CA1515/CA1852/CA1816/CA2214 to error — so the one PR that could break the
analyzer gate was the one PR that couldn't run it. Its green checks were CodeQL and
submit-nuget only. Main has been red since #98 merged (2026-07-14).

pool had the identical `.editorconfig` PR (#12) but its filter includes
`.editorconfig`, so its tests ran and passed. That path-filter line is the entire
difference.

## Fix

Adopt plumber's filter verbatim (both `push` and `pull_request`):

```yaml
paths: [ "src/**", "tests/**", "samples/**", "Directory.Build.props", "Directory.Build.targets", "Directory.Packages.props", ".editorconfig", ".gitattributes", "global.json", ".github/workflows/dotnet.tests.yml", ".github/actions/setup-dotnet/action.yml" ]
```

Also fold in the rest of the CI alignment already applied to pool in
marklauter/pool#13: workflow name/job naming, restore-then-build `--no-restore`,
pack the solution, pin the coverage artifact path, and re-encode any cp1252 `0x97`
bytes as UTF-8.

## Sequencing caveat

Applying this filter is what makes CI start running the gate — turning the hidden
errors into a red PR. Land the analyzer fixes ([[01-ca1515-public-test-classes]]
through [[05-ca2214-virtual-call-in-ctor]]) in the same change, or ahead of it, so
the branch is green when the gate first runs.
