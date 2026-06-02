  1. It re-reads immutable metadata on every operation. Key schema, attribute definitions, index definitions,
  the TTL attribute — these only change on control-plane calls (CreateTable, UpdateTable, UpdateTimeToLive).
  Yet every Put/Update/Delete/Batch/Transact/Query/Scan reads them back out of SQLite. The honest fix isn't
  a faster or combined query — it's to not read them: cache per-table metadata in-process (a
  ConcurrentDictionary), invalidated on those few DDL ops. That drops metadata reads to zero across the whole
  client, not just BatchWrite — and it makes my GetBatchWriteMetadataAsync moot. You don't optimize a read
  you've eliminated.

  2. It opens a fresh connection per store call. Every store.X() is new SqliteConnection → open (pooled, so
  cheap-ish) → re-apply PRAGMA synchronous/temp_store (+ your pragmas + initializer) → query → dispose. So
  the old BatchWrite was ~5 connection lifecycles — 4 reads + 1 write — each re-running the pragma setup,
  with the metadata reads and the write landing on different connections in different transactions. The lever
  is one connection (one transaction) per logical operation, threaded through the reads and the write:
  collapses N opens to 1 and closes the read-then-write gap. My change shrank N; it didn't touch the model.
  (Connection-per-call is a defensible simplicity choice, to be fair — it just isn't free.)



I have the full picture now. I traced every store call this method makes down to its connection/lock/query cost. Here's the analysis — no
  changes made yet.

  How BatchWriteItemAsync actually costs out

  Every store.* call in this method is a self-contained unit: acquire a lock → open a brand-new SqliteConnection → run PRAGMA 
  synchronous=NORMAL; temp_store=MEMORY → run the query → tear down (SqliteStore.cs:152, InMemorySqliteStore.cs:21). So "round-trips" here
  aren't cheap dictionary hits — each is a connection open + pragma command + SQL query under the RW-lock.

  Counting what the method issues per distinct table:

  ┌──────────────────────────┬──────────────┬─────────────────┐
  │           Call           │    Where     │      Reads      │
  ├──────────────────────────┼──────────────┼─────────────────┤
  │ GetKeySchemaAsync        │ loop 1, :125 │ tables row      │
  ├──────────────────────────┼──────────────┼─────────────────┤
  │ GetTtlAttributeNameAsync │ loop 1, :129 │ ttl_config row  │
  ├──────────────────────────┼──────────────┼─────────────────┤
  │ GetKeySchemaAsync again  │ loop 2, :167 │ same tables row │
  ├──────────────────────────┼──────────────┼─────────────────┤
  │ GetIndexDefinitionsAsync │ loop 2, :169 │ same tables row │
  └──────────────────────────┴──────────────┴─────────────────┘

  That's 4 read connections per table, then one write transaction (BatchWriteItemsAsync, :178). For the common single-table batch, you open 5
  SQLite connections to do what is logically one metadata fetch + one write.

  Findings, by impact

  1. The second GetKeySchemaAsync is pure waste (:167). Loop 2 re-fetches the key schema only to read keyInfo.AttributeDefinitions (:174) —
  which loop 1 already held in keyInfo and threw away. Capture it in loop 1 and this entire connection+lock+pragma+query disappears (25% of
  the read traffic). Bonus: it removes a redundant ResourceNotFoundException re-check and a TOCTOU window where a table dropped between the
  two loops would throw spuriously.

  2. The two passes should be one (:123 and :162). Loop 2 has no dependency on loop 1's results — both just enumerate request.RequestItems.
  Folding the index-info gathering into loop 1 is the natural place to reuse the captured keyInfo. Behavior is preserved: same dictionary
  iteration order, same exceptions, validation still precedes the write. Index loads have no side effects, so interleaving them with key
  validation is unobservable.

  3. GetKeySchemaAsync and GetIndexDefinitionsAsync read the same row. key_schema_json, attribute_definitions_json,
  global_secondary_indexes_json, local_secondary_indexes_json are all columns of the single tables row (SqliteStore.cs:364 vs :910). Two
  connections fetch four columns of one row. A combined store method (one SELECT, optionally LEFT JOIN ttl_config to fold in TTL too)
  collapses 4 reads → 1 read per table. This is the real structural win, but it's a store-API change, so it's the bigger/optional one.
  GetTableDescriptionAsync already selects all those columns, so the query shape exists.

  4. Dead guards + a pointless dictionary (:121, :128, :164). RequestItems is a Dictionary → keys are unique → each tableName is iterated
  exactly once. So if (!ttlConfigByTable.ContainsKey(tableName)) and indexInfoByTable?.ContainsKey(tableName) is true can never short-circuit.
  The whole ttlConfigByTable dictionary (alloc + two lookups per table) collapses to a local var ttlAttr = await ....

  5. Pre-size seenKeys (:119). operations is already sized to totalOps; seenKeys isn't. new HashSet<(string,string,string)>(totalOps) avoids
  resize churn — in line with the "allocation-aware" posture in the C# guidance.

  Findings 1, 2, 4, 5 are local to this method and behavior-preserving. The merged loop:

  var seenKeys = new HashSet<(string, string, string)>(totalOps);
  var operations = new List<BatchWriteOperation>(totalOps);
  Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable = null;

  foreach (var (tableName, writeRequests) in request.RequestItems)
  {
      var keyInfo = await store.GetKeySchemaAsync(tableName, cancellationToken)
          ?? throw new ResourceNotFoundException($"Requested resource not found: Table: {tableName} not found");

      var ttlAttr = await store.GetTtlAttributeNameAsync(tableName, cancellationToken);

      foreach (var writeRequest in writeRequests)
      {
          // ... unchanged put/delete handling ...
      }

      var indexes = await store.GetIndexDefinitionsAsync(tableName, cancellationToken);
      if (indexes.Count > 0)
      {
          indexInfoByTable ??= [];
          indexInfoByTable[tableName] = (indexes, keyInfo.AttributeDefinitions);
      }
  }

  await store.BatchWriteItemsAsync(operations, indexInfoByTable, cancellationToken);

  This takes per-table reads from 4 → 3 with zero behavior change. Finding 3 would take it to 1.

  One observation, out of scope

  BatchWriteItemsAsync recomputes item_count with a full COUNT(1) and table_size_bytes with a full SUM(LENGTH(item_json)) over the entire
  table, once per affected table per batch (SqliteStore.cs:783). For large tables that's the most expensive part of the whole operation —
  O(table size), not O(batch size). But it's shared write-path infrastructure (PutItemCore does the same) and switching to incremental
  maintenance is a cross-cutting design decision, so I'm flagging it rather than folding it into a "fix the batch method" change.

  ---
  Want me to apply findings 1/2/4/5 (safe, local, behavior-preserving), and optionally draft the combined store method for finding 3? I'd run
  the batch tests (BatchOperationsTests, BatchParityTests) after.