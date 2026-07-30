---
title: SQLite Lifetime
type: decision
summary: "A connection is opened per operation rather than held open, and an in-memory store holds a sentinel connection so the shared database survives between operations."
tags: [sqlite, connection-lifetime, configuration]
created: 2026-05-16
status: locked
---

# SQLite Lifetime

Each operation opens its own connection instead of holding one open for the client's lifetime. Setup work bypasses this: schema creation, WAL enable, and the in-memory keep-alive.

An in-memory store holds a sentinel connection for as long as the store lives. A `Mode=Memory;Cache=Shared` database is discarded once the last connection to it closes, so the sentinel is what keeps the shared database alive between operations.

The connection string selects the store: `:memory:` or `Mode=Memory` selects the in-memory store, anything else is file-backed. There is no default — the connection string is required, and `DynamoDbLiteOptionsBuilder.Build()` throws without one.
