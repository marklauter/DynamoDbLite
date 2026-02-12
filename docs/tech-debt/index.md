# Tech Debt Index

| # | Title | Priority | Status |
|---|-------|----------|--------|
| [001](001-extract-action-partial-record.md) | Extract action partial record | Low | Open |
| [002](002-transaction-pre-reads-not-atomic.md) | Transaction pre-reads not atomic | Medium | Won't Fix |
| [003](003-split-sqlitestore-by-mode.md) | Split SqliteStore by mode | Medium | Resolved |
| [004](004-cleanup-silent-exception.md) | Background TTL cleanup silently swallows exceptions | Medium | Open |
| [005](005-missing-ion-export-format.md) | Missing ION export format | Low | Open |
| [006](006-memorystream-toarray-copies.md) | MemoryStream .ToArray() avoidable copies | High | Open |
| [007](007-sql-string-concatenation.md) | SQL string concatenation in query hot paths | High | Open |
| [008](008-linq-to-loop-validation.md) | LINQ to loop replacements in validation | High | Resolved |
| [009](009-index-table-name-allocation.md) | IndexTableName string allocation | High | Open |
| [010](010-collection-presizing.md) | Collection pre-sizing | High | Resolved |
| [011](011-increment-prefix-char-array.md) | IncrementPrefix char array allocation | High | Resolved |
| [012](012-binary-set-toarray-in-lambdas.md) | Binary set .ToArray() in LINQ predicates | Medium | Open |
| [013](013-linq-select-tolist-serialization.md) | LINQ .Select().ToList() for serialization | Medium | Open |
| [014](014-dapper-aslist-copies.md) | Dapper .AsList() potential copies | Medium | Open |
| [015](015-future-perf-considerations.md) | Future performance considerations | Low | Open |
| [016](016-import-compression-not-implemented.md) | Import compression not applied | Low | Open |
| [017](017-empty-import-path-silent-success.md) | Empty import path silently succeeds | Medium | Open |
| [018](018-export-import-silent-exceptions.md) | Export/import background tasks swallow exceptions | Low | Open |
| [019](019-tag-resource-drops-existing-tags.md) | TagResourceAsync drops existing tags | High | Resolved |
| [020](020-increment-prefix-char-overflow.md) | IncrementPrefix char overflow breaks begins_with | High | Open |
| [021](021-empty-container-treated-as-null.md) | Empty containers treated as null in path resolution | Medium | Open |
| [022](022-empty-set-type-detection.md) | Empty set type detection fails | Medium | Open |
| [023](023-list-append-accepts-non-lists.md) | list_append silently accepts non-list operands | Medium | Open |
| [024](024-projection-creates-structure.md) | Projection creates structure instead of selecting | Medium | Open |
| [025](025-set-at-path-null-ref-on-list.md) | SetAtPath NullReferenceException on nested list | Low | Open |
| [026](026-between-null-comparison-throws.md) | BETWEEN with null operand throws | Low | Open |
| [027](027-lossy-empty-collection-serialization.md) | Lossy empty collection serialization | Medium | Resolved |
| [028](028-harden-apply-attribute-updates.md) | Harden ApplyAttributeUpdates legacy method | Medium | Open |
| [029](029-orm-test-gaps.md) | DynamoDBContext ORM test coverage gaps | Medium | Open |
