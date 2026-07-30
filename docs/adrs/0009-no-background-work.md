---
title: Export, import, and TTL cleanup run inline
summary: No operation defers work past its own return. Export and import complete before responding while still reporting IN_PROGRESS for API parity; the TTL sweep is awaited on the read path, throttled to once per table per minute. Supersedes the execution model described in ADR 0005 phases 7 and 10.
document:
  tags: [adr, ttl, export, import, concurrency]
  status: accepted
edge.supersedes: [docs/adrs/0005-implementation-phases.md]
---

# ADR 0009 — Export, import, and TTL cleanup run inline

Status: Accepted (supersedes the execution model in [ADR 0005](0005-implementation-phases.md) phases 7 and 10)

No DynamoDbLite operation defers work past its own return. Three call sites previously discarded a
task — `ExecuteExportAsync`, `ExecuteImportAsync`, and the TTL sweep — and all three now run to
completion within the call that starts them.

`ExportTableToPointInTime` and `ImportTable` still report `IN_PROGRESS` in their responses, and
`DescribeExport` / `DescribeImport` still report the terminal status. The status sequence a caller can
observe is unchanged; only the timing is.

## Why

Real DynamoDB returns `IN_PROGRESS` because the work continues on a server after the response is
sent. There is no server here. The work ran on the calling process either way, so deferring it bought
nothing and cost several things:

Failures were unobservable. A fire-and-forget task's exception went to a log nobody was reading, and
the operation's own caller had no way to learn the work never happened.

Disposal raced the work. `Dispose()` closed the store without waiting, so shipped code carried four
`catch (ObjectDisposedException)` blocks whose comments named test cleanup as the cause — production
code accommodating a test lifecycle. DynamoDbLite also runs as an in-process cache in production,
where the same race silently abandons a sweep or an export.

Behaviour was not reproducible. Whether a background task finished before the process moved on varied
run to run, which showed up as coverage drifting between 97.03% and 97.27% and hid the fact that the
export and import failure paths had never been tested at all.

For TTL specifically, reclamation was never load-bearing: every read path filters expired rows by
`ttl_epoch`, so an expired item is invisible the moment it expires whether or not the sweep has run.
That makes the sweep safe to move onto the read path — correctness does not depend on when it runs.

## Consequence

Export and import latency now lands on the caller. For the local databases this library targets that
is milliseconds, and it buys a terminal status that is true by the time the call returns. A caller
that polls `DescribeExport` sees `COMPLETED` on its first poll rather than after a delay.

The TTL sweep is throttled to once per table per 60 seconds, up from 30, so its cost amortises across
read traffic: one read per table per minute pays for the sweep and every other read returns after a
dictionary lookup. A sweep failure is logged and swallowed, because the caller's operation has already
succeeded on its own terms; `OperationCanceledException` propagates instead, since the sweep shares
the caller's token and a cancelled sweep means a cancelled request.

The four `ObjectDisposedException` accommodations are gone, and the throttle timestamp is still
stamped before the sweep runs, so a failed sweep consumes its window and reclamation waits another
minute. Reads stay correct throughout.

The trade-off accepted: an export of a very large table blocks its caller where real DynamoDB would
not. That is the cost of the work being observable, and it is bounded by local disk rather than a
network round trip.
