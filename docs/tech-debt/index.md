# Tech Debt Index

| # | Title | Type | Priority | Status |
|---|-------|------|----------|--------|
| [001](001-extract-action-partial-record.md) | Extract action partial record | Cleanup | Low | Open |
| [002](002-transaction-pre-reads-not-atomic.md) | Transaction pre-reads not atomic | Bug | Medium | Won't Fix |
| [003](003-split-sqlitestore-by-mode.md) | Split SqliteStore by mode | Cleanup | Medium | Resolved |
| [004](004-cleanup-silent-exception.md) | Background TTL cleanup silently swallows exceptions | Observability | Medium | Open |
| [005](005-missing-ion-export-format.md) | Missing ION export format | Feature Gap | Low | Open |
| [006](006-memorystream-toarray-copies.md) | MemoryStream .ToArray() avoidable copies | Performance | High | Resolved |
| [007](007-sql-string-concatenation.md) | SQL string concatenation in query hot paths | Performance | High | Resolved |
| [008](008-linq-to-loop-validation.md) | LINQ to loop replacements in validation | Performance | High | Resolved |
| [009](009-index-table-name-allocation.md) | IndexTableName string allocation | Performance | High | Resolved |
| [010](010-collection-presizing.md) | Collection pre-sizing | Performance | High | Resolved |
| [011](011-increment-prefix-char-array.md) | IncrementPrefix char array allocation | Performance | High | Resolved |
| [012](012-binary-set-toarray-in-lambdas.md) | Binary set .ToArray() in LINQ predicates | Performance | Medium | Resolved |
| [013](013-linq-select-tolist-serialization.md) | LINQ .Select().ToList() for serialization | Performance | Medium | Resolved |
| [014](014-dapper-aslist-copies.md) | Dapper .AsList() potential copies | Performance | Medium | Open |
| [015](015-future-perf-considerations.md) | Future performance considerations | Performance | Low | Open |
| [016](016-import-compression-not-implemented.md) | Import compression not applied | Feature Gap | Low | Open |
| [017](017-empty-import-path-silent-success.md) | Empty import path silently succeeds | Validation | Medium | Open |
| [018](018-export-import-silent-exceptions.md) | Export/import background tasks swallow exceptions | Observability | Low | Open |
| [019](019-tag-resource-drops-existing-tags.md) | TagResourceAsync drops existing tags | Bug | High | Resolved |
| [020](020-increment-prefix-char-overflow.md) | IncrementPrefix char overflow breaks begins_with | Bug | High | Resolved |
| [021](021-empty-container-treated-as-null.md) | Empty containers treated as null in path resolution | Bug | Medium | Resolved |
| [022](022-empty-set-type-detection.md) | Empty set type detection fails | Bug | Medium | Resolved |
| [023](023-list-append-accepts-non-lists.md) | list_append silently accepts non-list operands | Bug | Medium | Resolved |
| [024](024-projection-creates-structure.md) | Projection creates structure instead of selecting | Bug | Medium | Resolved |
| [025](025-set-at-path-null-ref-on-list.md) | SetAtPath NullReferenceException on nested list | Safety | Low | Open |
| [026](026-between-null-comparison-throws.md) | BETWEEN with null operand throws | Bug | Low | Open |
| [027](027-lossy-empty-collection-serialization.md) | Lossy empty collection serialization | Bug | Medium | Resolved |
| [028](028-harden-apply-attribute-updates.md) | Harden ApplyAttributeUpdates legacy method | Bug | Medium | Open |
| [029](029-orm-test-gaps.md) | DynamoDBContext ORM test coverage gaps | Testing | Medium | Open |
