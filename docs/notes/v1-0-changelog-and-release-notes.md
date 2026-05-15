# v1.0 CHANGELOG and release notes

Tags: release, v1.0, docs
Author CHANGELOG.md at repo root with the v1.0 entry — covered scenarios, known limitations, deferred features.


## Observation

No `CHANGELOG.md` at the repo root as of this work. `docs/parity.md` carries the parity Coverage / Deferred lists; that's the source for the v1.0 entry but isn't a release note.

## Interpretation

Release notes are the first thing a NuGet consumer reads when evaluating an update. The format consumers expect is a CHANGELOG following Keep-a-Changelog conventions — `## [1.0.0] - <date>` with sections like `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`. For a v1.0 launch the relevant sections are `Added` (everything that ships) and a `Known limitations` / `Deferred` section that mirrors `docs/parity.md`.

## Next

Create `CHANGELOG.md` at repo root. v1.0.0 entry sections:

- **API surface** — DynamoDB low-level v3 API (point at the supported method list; e.g. `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `TransactWriteItems`, `TransactGetItems`, `BatchGetItem`, `BatchWriteItem`, table management, etc.).
- **Parity coverage** — link to `docs/parity.md` Coverage list.
- **Known limitations** — parallel-scan + Limit interaction (see [parallel-scan-limit-interaction](parallel-scan-limit-interaction.md)), anything else surfaced before tag.
- **Deferred to future releases** — TTL parity, Export/Import parity, real AWS cloud backend; per `docs/parity.md` "Deferred indefinitely".

Link CHANGELOG.md from README and from the NuGet package's `PackageReleaseNotes` property.
