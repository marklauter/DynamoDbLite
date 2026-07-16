---
title: Clean up and delete the remediation folder when all work is done
summary: Once every remediation todo above is closed and build-gate.sh is green, delete docs/remediation/ — it is scaffolding, not permanent docs.
tags: [todo, remediation, cleanup]
created: 2026-07-16
priority: low
effort: low
status: open
blocked_by:
  - "[[01-ca1515-public-test-classes]]"
  - "[[02-ca1307-assert-stringcomparison]]"
  - "[[03-ca1852-seal-test-model-types]]"
  - "[[04-ca1816-disposeasync-suppressfinalize]]"
  - "[[05-ca2214-virtual-call-in-ctor]]"
  - "[[06-align-ci-path-filter-to-canon]]"
---

This folder tracks the one-time remediation of the silently-red main. It is
scaffolding, not part of the permanent `docs/notes/` corpus.

## Done when

1. Items 01–06 are all `status: closed`.
2. `build-gate.sh` is green — format, build, test, coverage ratchet all pass.
3. DynamoDbLite CI runs the `format` and `test` jobs on push/PR and passes.

Then delete `docs/remediation/` in the same commit that closes out the work, and
confirm nothing links into it from `docs/notes/`.
