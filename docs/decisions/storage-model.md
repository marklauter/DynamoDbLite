---
title: Storage Model
type: decision
summary: "Every item lives in one `items` table keyed by `(table_name, pk, sk)` with the body as JSON, plus denormalized `sk_num` and `ttl_epoch` columns for ordered range queries and read-time TTL filtering; each secondary index gets its own table."
tags: [storage, sqlite, schema]
created: 2026-05-16
status: locked
---

# Storage Model

All data lives in SQLite. The schema centers on an `items` table with `(table_name, pk, sk)` as primary key and a JSON payload column. Two denormalized columns sit alongside it: `sk_num` holds the numeric sort key for ordered range queries, and `ttl_epoch` drives read-time TTL filtering. Table metadata, TTL config, tags, exports, imports, and per-index tables are separate.

Each GSI and LSI gets its own `idx_{tableName}_{indexName}` table. For the column-by-column schema, see [Storage Architecture](https://github.com/marklauter/DynamoDbLite/wiki/Storage-Architecture) in the wiki.
