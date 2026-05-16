# ADR 0001 — Storage Model

Status: Accepted

All data lives in SQLite. The schema centres on an `items` table with `(table_name, pk, sk)` as primary key and a JSON payload column, plus a denormalized numeric sort-key column (`sk_num`) for ordered range queries and a `ttl_epoch` column for read-time TTL filtering. Table metadata, TTL config, tags, exports, imports, and per-index tables are separate.

For the full schema (column-by-column, plus the per-GSI/LSI `idx_{tableName}_{indexName}` tables), see [Storage Architecture](https://github.com/marklauter/DynamoDbLite/wiki/Storage-Architecture) in the wiki.
