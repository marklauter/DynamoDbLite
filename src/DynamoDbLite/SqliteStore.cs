using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;

namespace DynamoDbLite;

internal sealed class SqliteStore : IDisposable
{
    private readonly string connectionString;
    private readonly SqliteConnection sentinel;
    private readonly bool isMemory;
    private readonly ConcurrentDictionary<string, DateTime> lastCleanupByTable = new();
    private bool walEnabled;
    private bool disposed;

    internal SqliteStore(DynamoDbLiteOptions options)
    {
        var builder = new SqliteConnectionStringBuilder(options.ConnectionString)
        {
            Pooling = true,
            Mode = SqliteOpenMode.ReadWriteCreate,
            ForeignKeys = true,
        };

        isMemory = builder.Mode is SqliteOpenMode.Memory
            || builder.DataSource.Contains(":memory:", StringComparison.OrdinalIgnoreCase);
        connectionString = builder.ToString();

        sentinel = new SqliteConnection(connectionString);
        sentinel.Open();

        if (!isMemory)
        {
            using var walCommand = sentinel.CreateCommand();
            walCommand.CommandText = "PRAGMA journal_mode = 'wal'";
            _ = walCommand.ExecuteNonQuery();
            walEnabled = true;
        }

        InitializeSchema();
    }

    private void InitializeSchema()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        _ = connection.Execute("""
            CREATE TABLE IF NOT EXISTS tables (
                table_name                      TEXT NOT NULL PRIMARY KEY,
                key_schema_json                 TEXT NOT NULL,
                attribute_definitions_json       TEXT NOT NULL,
                provisioned_throughput_json      TEXT NOT NULL DEFAULT '{}',
                global_secondary_indexes_json   TEXT NOT NULL DEFAULT '[]',
                local_secondary_indexes_json    TEXT NOT NULL DEFAULT '[]',
                created_at                      TEXT NOT NULL,
                status                          TEXT NOT NULL DEFAULT 'ACTIVE',
                item_count                      INTEGER NOT NULL DEFAULT 0,
                table_size_bytes                INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS items (
                table_name  TEXT NOT NULL,
                pk          TEXT NOT NULL,
                sk          TEXT NOT NULL DEFAULT '',
                sk_num      REAL,
                ttl_epoch   REAL,
                item_json   TEXT NOT NULL,
                PRIMARY KEY (table_name, pk, sk)
            );

            CREATE TABLE IF NOT EXISTS ttl_config (
                table_name      TEXT PRIMARY KEY,
                attribute_name  TEXT NOT NULL
            );
            """);

        // Migration for existing file-based DBs that lack ttl_epoch
        try
        {
            _ = connection.Execute("ALTER TABLE items ADD COLUMN ttl_epoch REAL");
        }
        catch (SqliteException)
        {
            // Column already exists
        }
    }

    internal async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        if (!isMemory && !walEnabled)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA journal_mode = 'wal'";
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
            walEnabled = true;
        }

        return connection;
    }

    internal static double NowEpoch() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    // ── TTL config CRUD ──────────────────────────────────────────────

    internal async Task SetTtlConfigAsync(string tableName, string attributeName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        _ = await connection.ExecuteAsync(
            """
            INSERT INTO ttl_config (table_name, attribute_name) VALUES (@tableName, @attributeName)
            ON CONFLICT (table_name) DO UPDATE SET attribute_name = @attributeName
            """,
            new { tableName, attributeName });
    }

    internal async Task RemoveTtlConfigAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        _ = await connection.ExecuteAsync(
            "DELETE FROM ttl_config WHERE table_name = @tableName",
            new { tableName });
    }

    internal async Task<string?> GetTtlAttributeNameAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT attribute_name FROM ttl_config WHERE table_name = @tableName",
            new { tableName });
    }

    internal async Task<bool> TableExistsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteScalarAsync<int>(
            "SELECT COUNT(1) FROM tables WHERE table_name = @tableName",
            new { tableName }) > 0;
    }

    internal async Task CreateTableAsync(
        string tableName,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions,
        ProvisionedThroughput? provisionedThroughput,
        List<IndexDefinition>? gsiDefinitions,
        List<IndexDefinition>? lsiDefinitions,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        var keySchemaJson = JsonSerializer.Serialize(keySchema.Select(static k =>
            new { k.AttributeName, KeyType = k.KeyType.Value }).ToList());
        var attrDefsJson = JsonSerializer.Serialize(attributeDefinitions.Select(static a =>
            new { a.AttributeName, AttributeType = a.AttributeType.Value }).ToList());
        var throughputJson = provisionedThroughput is not null
            ? JsonSerializer.Serialize(new
            {
                provisionedThroughput.ReadCapacityUnits,
                provisionedThroughput.WriteCapacityUnits
            })
            : "{}";
        var gsiJson = SerializeIndexDefinitions(gsiDefinitions ?? []);
        var lsiJson = SerializeIndexDefinitions(lsiDefinitions ?? []);

        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        _ = await connection.ExecuteAsync("""
            INSERT INTO tables (table_name, key_schema_json, attribute_definitions_json, provisioned_throughput_json, global_secondary_indexes_json, local_secondary_indexes_json, created_at, status)
            VALUES (@tableName, @keySchemaJson, @attrDefsJson, @throughputJson, @gsiJson, @lsiJson, @now, 'ACTIVE')
            """,
            new { tableName, keySchemaJson, attrDefsJson, throughputJson, gsiJson, lsiJson, now },
            transaction);

        foreach (var idx in gsiDefinitions ?? [])
            await CreateIndexTableAsync(connection, tableName, idx.IndexName, transaction);

        foreach (var idx in lsiDefinitions ?? [])
            await CreateIndexTableAsync(connection, tableName, idx.IndexName, transaction);

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var indexes = await GetIndexDefinitionsAsync(connection, tableName, transaction);
        foreach (var idx in indexes)
            await DropIndexTableAsync(connection, tableName, idx.IndexName, transaction);

        _ = await connection.ExecuteAsync(
            "DELETE FROM items WHERE table_name = @tableName",
            new { tableName },
            transaction);

        _ = await connection.ExecuteAsync(
            "DELETE FROM tables WHERE table_name = @tableName",
            new { tableName },
            transaction);

        _ = await connection.ExecuteAsync(
            "DELETE FROM ttl_config WHERE table_name = @tableName",
            new { tableName },
            transaction);

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task<TableDescription?> GetTableDescriptionAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<TableRow>(
            """
            SELECT
                table_name                      AS TableName,
                key_schema_json                 AS KeySchemaJson,
                attribute_definitions_json       AS AttributeDefinitionsJson,
                provisioned_throughput_json      AS ProvisionedThroughputJson,
                global_secondary_indexes_json   AS GlobalSecondaryIndexesJson,
                local_secondary_indexes_json    AS LocalSecondaryIndexesJson,
                created_at                      AS CreatedAt,
                status                          AS Status,
                item_count                      AS ItemCount,
                table_size_bytes                AS TableSizeBytes
            FROM tables
            WHERE table_name = @tableName
            """,
            new { tableName });

        return row is null ? null : ToTableDescription(row);
    }

    internal async Task<KeySchemaInfo?> GetKeySchemaAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<KeySchemaRow>(
            """
            SELECT
                key_schema_json             AS KeySchemaJson,
                attribute_definitions_json   AS AttributeDefinitionsJson
            FROM tables
            WHERE table_name = @tableName
            """,
            new { tableName });

        return row is null
            ? null
            : new KeySchemaInfo(DeserializeKeySchema(row.KeySchemaJson), DeserializeAttributeDefinitions(row.AttributeDefinitionsJson));
    }

    internal async Task<string?> PutItemAsync(string tableName, string pk, string sk, string itemJson, double? skNum = null, double? ttlEpoch = null, double? nowEpoch = null, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await PutItemCoreAsync(connection, transaction, tableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch ?? NowEpoch());
        await transaction.CommitAsync(cancellationToken);
        return oldJson;
    }

    private static async Task<string?> PutItemCoreAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string pk,
        string sk,
        string itemJson,
        double? skNum,
        double? ttlEpoch,
        double nowEpoch)
    {
        var oldJson = await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)",
            new { tableName, pk, sk, nowEpoch },
            transaction);

        _ = await connection.ExecuteAsync(
            """
            INSERT INTO items (table_name, pk, sk, sk_num, ttl_epoch, item_json)
            VALUES (@tableName, @pk, @sk, @skNum, @ttlEpoch, @itemJson)
            ON CONFLICT (table_name, pk, sk) DO UPDATE SET item_json = @itemJson, sk_num = @skNum, ttl_epoch = @ttlEpoch
            """,
            new { tableName, pk, sk, skNum, ttlEpoch, itemJson },
            transaction);

        _ = await connection.ExecuteAsync(
            """
            UPDATE tables SET
                item_count = (SELECT COUNT(1) FROM items WHERE table_name = @tableName),
                table_size_bytes = (SELECT COALESCE(SUM(LENGTH(item_json)), 0) FROM items WHERE table_name = @tableName)
            WHERE table_name = @tableName
            """,
            new { tableName },
            transaction);

        return oldJson;
    }

    internal async Task<string?> GetItemAsync(string tableName, string pk, string sk, double? nowEpoch = null, CancellationToken cancellationToken = default)
    {
        var epoch = nowEpoch ?? NowEpoch();
        using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)",
            new { tableName, pk, sk, nowEpoch = epoch });
    }

    internal async Task<string?> DeleteItemAsync(string tableName, string pk, string sk, double? nowEpoch = null, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await DeleteItemCoreAsync(connection, transaction, tableName, pk, sk, nowEpoch);
        await transaction.CommitAsync(cancellationToken);
        return oldJson;
    }

    private static async Task<string?> DeleteItemCoreAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string pk,
        string sk,
        double? nowEpoch = null)
    {
        var epoch = nowEpoch ?? NowEpoch();
        var oldJson = await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)",
            new { tableName, pk, sk, nowEpoch = epoch },
            transaction);

        if (oldJson is not null)
        {
            _ = await connection.ExecuteAsync(
                "DELETE FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
                new { tableName, pk, sk },
                transaction);

            _ = await connection.ExecuteAsync(
                """
                UPDATE tables SET
                    item_count = (SELECT COUNT(1) FROM items WHERE table_name = @tableName),
                    table_size_bytes = (SELECT COALESCE(SUM(LENGTH(item_json)), 0) FROM items WHERE table_name = @tableName)
                WHERE table_name = @tableName
                """,
                new { tableName },
                transaction);
        }

        return oldJson;
    }

    internal async Task<List<ItemRow>> QueryItemsAsync(
        string tableName,
        string pkValue,
        string? skWhereSql,
        DynamicParameters? skParams,
        string orderByColumn,
        bool ascending,
        int? limit,
        string? exclusiveStartSk,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var parameters = new DynamicParameters();
        parameters.Add("@tableName", tableName);
        parameters.Add("@pk", pkValue);
        parameters.Add("@nowEpoch", nowEpoch ?? NowEpoch());

        if (skParams is not null)
            parameters.AddDynamicParams(skParams);

        var sql = "SELECT pk AS Pk, sk AS Sk, item_json AS ItemJson FROM items WHERE table_name = @tableName AND pk = @pk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)";

        if (skWhereSql is not null)
            sql += $" AND {skWhereSql}";

        if (exclusiveStartSk is not null)
        {
            var skOp = ascending ? ">" : "<";
            parameters.Add("@esSk", exclusiveStartSk);
            sql += $" AND {orderByColumn} {skOp} @esSk";
        }

        var direction = ascending ? "ASC" : "DESC";
        sql += $" ORDER BY {orderByColumn} {direction}";

        if (limit is not null)
        {
            parameters.Add("@limit", limit.Value);
            sql += " LIMIT @limit";
        }

        return (await connection.QueryAsync<ItemRow>(sql, parameters)).AsList();
    }

    internal async Task<List<ItemRow>> ScanItemsAsync(
        string tableName,
        int? limit,
        string? exclusiveStartPk,
        string? exclusiveStartSk,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var parameters = new DynamicParameters();
        parameters.Add("@tableName", tableName);
        parameters.Add("@nowEpoch", nowEpoch ?? NowEpoch());

        var sql = "SELECT pk AS Pk, sk AS Sk, item_json AS ItemJson FROM items WHERE table_name = @tableName AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)";

        if (exclusiveStartPk is not null)
        {
            parameters.Add("@esPk", exclusiveStartPk);
            parameters.Add("@esSk", exclusiveStartSk ?? string.Empty);
            sql += " AND (pk > @esPk OR (pk = @esPk AND sk > @esSk))";
        }

        sql += " ORDER BY pk, sk";

        if (limit is not null)
        {
            parameters.Add("@limit", limit.Value);
            sql += " LIMIT @limit";
        }

        return (await connection.QueryAsync<ItemRow>(sql, parameters)).AsList();
    }

    internal async Task<List<string>> ListTableNamesAsync(string? exclusiveStartTableName, int limit, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        if (exclusiveStartTableName is not null)
        {
            return (await connection.QueryAsync<string>(
                "SELECT table_name FROM tables WHERE table_name > @exclusiveStartTableName ORDER BY table_name LIMIT @limit",
                new { exclusiveStartTableName, limit })).AsList();
        }

        return (await connection.QueryAsync<string>(
            "SELECT table_name FROM tables ORDER BY table_name LIMIT @limit",
            new { limit })).AsList();
    }

    private static TableDescription ToTableDescription(TableRow row)
    {
        var keySchema = DeserializeKeySchema(row.KeySchemaJson);
        var attributeDefinitions = DeserializeAttributeDefinitions(row.AttributeDefinitionsJson);
        var throughput = DeserializeProvisionedThroughput(row.ProvisionedThroughputJson);
        var createdAt = DateTime.Parse(row.CreatedAt, CultureInfo.InvariantCulture);
        var gsiDefs = DeserializeIndexDefinitions(row.GlobalSecondaryIndexesJson);
        var lsiDefs = DeserializeIndexDefinitions(row.LocalSecondaryIndexesJson);

        var description = new TableDescription
        {
            TableName = row.TableName,
            TableStatus = row.Status,
            KeySchema = keySchema,
            AttributeDefinitions = attributeDefinitions,
            ProvisionedThroughput = throughput is not null
                ? new ProvisionedThroughputDescription
                {
                    ReadCapacityUnits = throughput.ReadCapacityUnits,
                    WriteCapacityUnits = throughput.WriteCapacityUnits,
                    LastIncreaseDateTime = createdAt,
                    LastDecreaseDateTime = createdAt
                }
                : new ProvisionedThroughputDescription(),
            CreationDateTime = createdAt,
            ItemCount = row.ItemCount,
            TableSizeBytes = row.TableSizeBytes,
            TableArn = $"arn:aws:dynamodb:local:000000000000:table/{row.TableName}"
        };

        if (gsiDefs.Count > 0)
        {
            description.GlobalSecondaryIndexes = gsiDefs.Select(idx => new GlobalSecondaryIndexDescription
            {
                IndexName = idx.IndexName,
                KeySchema = idx.KeySchema,
                Projection = ToProjection(idx),
                IndexStatus = IndexStatus.ACTIVE,
                IndexSizeBytes = 0,
                ItemCount = 0,
                IndexArn = $"arn:aws:dynamodb:local:000000000000:table/{row.TableName}/index/{idx.IndexName}",
                ProvisionedThroughput = new ProvisionedThroughputDescription()
            }).ToList();
        }

        if (lsiDefs.Count > 0)
        {
            description.LocalSecondaryIndexes = lsiDefs.Select(idx => new LocalSecondaryIndexDescription
            {
                IndexName = idx.IndexName,
                KeySchema = idx.KeySchema,
                Projection = ToProjection(idx),
                IndexSizeBytes = 0,
                ItemCount = 0,
                IndexArn = $"arn:aws:dynamodb:local:000000000000:table/{row.TableName}/index/{idx.IndexName}"
            }).ToList();
        }

        return description;
    }

    private static Projection ToProjection(IndexDefinition idx) =>
        new()
        {
            ProjectionType = idx.ProjectionType,
            NonKeyAttributes = idx.NonKeyAttributes is { Count: > 0 } ? idx.NonKeyAttributes : null
        };

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<KeySchemaElement> DeserializeKeySchema(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var result = new List<KeySchemaElement>();
        foreach (var e in doc.RootElement.EnumerateArray())
        {
            result.Add(new KeySchemaElement
            {
                AttributeName = e.GetProperty("AttributeName").GetString()!,
                KeyType = e.GetProperty("KeyType").GetString()!
            });
        }

        return result;
    }

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<AttributeDefinition> DeserializeAttributeDefinitions(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var result = new List<AttributeDefinition>();
        foreach (var e in doc.RootElement.EnumerateArray())
        {
            result.Add(new AttributeDefinition
            {
                AttributeName = e.GetProperty("AttributeName").GetString()!,
                AttributeType = e.GetProperty("AttributeType").GetString()!
            });
        }

        return result;
    }

    private static ProvisionedThroughput? DeserializeProvisionedThroughput(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return root.ValueKind is JsonValueKind.Object
            && root.TryGetProperty("ReadCapacityUnits", out var rcu)
            && root.TryGetProperty("WriteCapacityUnits", out var wcu)
            ? new ProvisionedThroughput
            {
                ReadCapacityUnits = rcu.GetInt64(),
                WriteCapacityUnits = wcu.GetInt64()
            }
            : null;
    }

    internal async Task<List<(string TableName, string ItemJson)>> BatchGetItemsAsync(
        List<(string TableName, string Pk, string Sk)> keys,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var epoch = nowEpoch ?? NowEpoch();
        var results = new List<(string TableName, string ItemJson)>(keys.Count);

        foreach (var (tableName, pk, sk) in keys)
        {
            var itemJson = await connection.QuerySingleOrDefaultAsync<string>(
                "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)",
                new { tableName, pk, sk, nowEpoch = epoch });

            if (itemJson is not null)
                results.Add((tableName, itemJson));
        }

        return results;
    }

    internal async Task BatchWriteItemsAsync(
        List<BatchWriteOperation> operations,
        Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var op in operations)
        {
            string? oldJson = null;

            if (op.ItemJson is not null)
            {
                oldJson = await connection.QuerySingleOrDefaultAsync<string>(
                    "SELECT item_json FROM items WHERE table_name = @TableName AND pk = @Pk AND sk = @Sk",
                    new { op.TableName, op.Pk, op.Sk },
                    transaction);

                _ = await connection.ExecuteAsync(
                    """
                    INSERT INTO items (table_name, pk, sk, sk_num, ttl_epoch, item_json)
                    VALUES (@TableName, @Pk, @Sk, @SkNum, @TtlEpoch, @ItemJson)
                    ON CONFLICT (table_name, pk, sk) DO UPDATE SET item_json = @ItemJson, sk_num = @SkNum, ttl_epoch = @TtlEpoch
                    """,
                    op,
                    transaction);
            }
            else
            {
                oldJson = await connection.QuerySingleOrDefaultAsync<string>(
                    "SELECT item_json FROM items WHERE table_name = @TableName AND pk = @Pk AND sk = @Sk",
                    new { op.TableName, op.Pk, op.Sk },
                    transaction);

                _ = await connection.ExecuteAsync(
                    "DELETE FROM items WHERE table_name = @TableName AND pk = @Pk AND sk = @Sk",
                    op,
                    transaction);
            }

            if (indexInfoByTable is not null
                && indexInfoByTable.TryGetValue(op.TableName, out var info)
                && info.Indexes.Count > 0)
            {
                var newItem = op.ItemJson is not null
                    ? AttributeValueSerializer.Deserialize(op.ItemJson)
                    : null;

                await MaintainIndexesAsync(
                    connection, transaction, op.TableName, op.Pk, op.Sk,
                    info.Indexes, info.AttrDefs, newItem, oldJson, op.TtlEpoch);
            }
        }

        var affectedTables = operations.Select(static o => o.TableName).Distinct();
        foreach (var tableName in affectedTables)
        {
            _ = await connection.ExecuteAsync(
                """
                UPDATE tables SET
                    item_count = (SELECT COUNT(1) FROM items WHERE table_name = @tableName),
                    table_size_bytes = (SELECT COALESCE(SUM(LENGTH(item_json)), 0) FROM items WHERE table_name = @tableName)
                WHERE table_name = @tableName
                """,
                new { tableName },
                transaction);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task TransactWriteItemsAsync(
        List<TransactWriteOperation> operations,
        Dictionary<string, (List<IndexDefinition> Indexes, List<AttributeDefinition> AttrDefs)>? indexInfoByTable,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        var epoch = nowEpoch ?? NowEpoch();
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var op in operations)
        {
            var oldJson = op.IsDelete
                ? await DeleteItemCoreAsync(connection, transaction, op.TableName, op.Pk, op.Sk, epoch)
                : await PutItemCoreAsync(connection, transaction, op.TableName, op.Pk, op.Sk, op.ItemJson!, op.SkNum, op.TtlEpoch, epoch);

            if (indexInfoByTable is not null
                && indexInfoByTable.TryGetValue(op.TableName, out var info)
                && info.Indexes.Count > 0)
            {
                var newItem = op.IsDelete ? null
                    : AttributeValueSerializer.Deserialize(op.ItemJson!);

                await MaintainIndexesAsync(
                    connection, transaction, op.TableName, op.Pk, op.Sk,
                    info.Indexes, info.AttrDefs, newItem, oldJson, op.TtlEpoch);
            }
        }

        // PutItemCoreAsync/DeleteItemCoreAsync already update table stats per operation,
        // so no additional stats update is needed here.
        await transaction.CommitAsync(cancellationToken);
    }

    // ── Index table management ────────────────────────────────────

    internal static string IndexTableName(string tableName, string indexName) =>
        $"idx_{tableName}_{indexName}";

    internal static async Task CreateIndexTableAsync(
        DbConnection connection,
        string tableName,
        string indexName,
        DbTransaction? transaction)
    {
        var idxTable = IndexTableName(tableName, indexName);
        _ = await connection.ExecuteAsync(
            $"""
            CREATE TABLE IF NOT EXISTS "{idxTable}" (
                pk          TEXT NOT NULL,
                sk          TEXT NOT NULL DEFAULT '',
                sk_num      REAL,
                table_pk    TEXT NOT NULL,
                table_sk    TEXT NOT NULL DEFAULT '',
                ttl_epoch   REAL,
                item_json   TEXT NOT NULL,
                PRIMARY KEY (pk, sk, table_pk, table_sk)
            )
            """,
            transaction: transaction);
    }

    internal static async Task DropIndexTableAsync(
        DbConnection connection,
        string tableName,
        string indexName,
        DbTransaction? transaction)
    {
        var idxTable = IndexTableName(tableName, indexName);
        _ = await connection.ExecuteAsync(
            $"""DROP TABLE IF EXISTS "{idxTable}" """,
            transaction: transaction);
    }

    internal async Task<List<IndexDefinition>> GetIndexDefinitionsAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        return await GetIndexDefinitionsAsync(connection, tableName, null);
    }

    private static async Task<List<IndexDefinition>> GetIndexDefinitionsAsync(
        DbConnection connection,
        string tableName,
        DbTransaction? transaction)
    {
        var row = await connection.QuerySingleOrDefaultAsync<IndexMetadataRow>(
            """
            SELECT
                global_secondary_indexes_json   AS GlobalSecondaryIndexesJson,
                local_secondary_indexes_json    AS LocalSecondaryIndexesJson
            FROM tables
            WHERE table_name = @tableName
            """,
            new { tableName },
            transaction);

        if (row is null)
            return [];

        var gsis = DeserializeIndexDefinitions(row.GlobalSecondaryIndexesJson);
        var lsis = DeserializeIndexDefinitions(row.LocalSecondaryIndexesJson);
        return [.. gsis, .. lsis];
    }

    internal async Task<KeySchemaInfo?> GetIndexKeySchemaAsync(
        string tableName,
        string indexName,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<KeySchemaRow>(
            """
            SELECT
                key_schema_json             AS KeySchemaJson,
                attribute_definitions_json   AS AttributeDefinitionsJson
            FROM tables
            WHERE table_name = @tableName
            """,
            new { tableName });

        if (row is null)
            return null;

        var attrDefs = DeserializeAttributeDefinitions(row.AttributeDefinitionsJson);
        var indexes = await GetIndexDefinitionsAsync(connection, tableName, null);
        var indexDef = indexes.FirstOrDefault(i => i.IndexName == indexName);
        return indexDef is null ? null : new KeySchemaInfo(indexDef.KeySchema, attrDefs);
    }

    // ── Index data operations ──────────────────────────────────────

    internal static async Task UpsertIndexEntryAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string indexName,
        string indexPk,
        string indexSk,
        double? indexSkNum,
        string tablePk,
        string tableSk,
        string itemJson,
        double? ttlEpoch = null)
    {
        var idxTable = IndexTableName(tableName, indexName);
        _ = await connection.ExecuteAsync(
            $"""
            INSERT INTO "{idxTable}" (pk, sk, sk_num, table_pk, table_sk, ttl_epoch, item_json)
            VALUES (@indexPk, @indexSk, @indexSkNum, @tablePk, @tableSk, @ttlEpoch, @itemJson)
            ON CONFLICT (pk, sk, table_pk, table_sk) DO UPDATE SET item_json = @itemJson, sk_num = @indexSkNum, ttl_epoch = @ttlEpoch
            """,
            new { indexPk, indexSk, indexSkNum, tablePk, tableSk, ttlEpoch, itemJson },
            transaction);
    }

    internal static async Task DeleteIndexEntryAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string indexName,
        string tablePk,
        string tableSk)
    {
        var idxTable = IndexTableName(tableName, indexName);
        _ = await connection.ExecuteAsync(
            $"""DELETE FROM "{idxTable}" WHERE table_pk = @tablePk AND table_sk = @tableSk""",
            new { tablePk, tableSk },
            transaction);
    }

    internal static async Task MaintainIndexesAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string tablePk,
        string tableSk,
        List<IndexDefinition> indexes,
        List<AttributeDefinition> attributeDefinitions,
        Dictionary<string, AttributeValue>? newItem,
        string? oldItemJson,
        double? ttlEpoch = null)
    {
        foreach (var idx in indexes)
        {
            // Always delete the old entry
            if (oldItemJson is not null)
                await DeleteIndexEntryAsync(connection, transaction, tableName, idx.IndexName, tablePk, tableSk);

            // Insert new entry if item exists and has the required key attributes
            if (newItem is not null)
            {
                var keys = KeyHelper.TryExtractIndexKeys(newItem, idx.KeySchema, attributeDefinitions);
                if (keys is not null)
                {
                    var skNum = ComputeIndexSkNum(keys.Value.Sk, idx.KeySchema, attributeDefinitions);
                    var itemJson = AttributeValueSerializer.Serialize(newItem);
                    await UpsertIndexEntryAsync(
                        connection, transaction, tableName, idx.IndexName,
                        keys.Value.Pk, keys.Value.Sk, skNum, tablePk, tableSk, itemJson, ttlEpoch);
                }
            }
        }
    }

    internal static double? ComputeIndexSkNum(
        string sk,
        List<KeySchemaElement> keySchema,
        List<AttributeDefinition> attributeDefinitions)
    {
        var rangeKey = keySchema.FirstOrDefault(static k => k.KeyType == KeyType.RANGE);
        if (rangeKey is null)
            return null;

        var attrDef = attributeDefinitions.First(a => a.AttributeName == rangeKey.AttributeName);
        return attrDef.AttributeType == ScalarAttributeType.N
            ? double.Parse(sk, CultureInfo.InvariantCulture)
            : null;
    }

    // ── Index query/scan ───────────────────────────────────────────

    internal async Task<List<IndexItemRow>> QueryIndexItemsAsync(
        string tableName,
        string indexName,
        string pkValue,
        string? skWhereSql,
        DynamicParameters? skParams,
        string orderByColumn,
        bool ascending,
        int? limit,
        string? exclusiveStartSk,
        string? exclusiveStartTablePk,
        string? exclusiveStartTableSk,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var idxTable = IndexTableName(tableName, indexName);
        var parameters = new DynamicParameters();
        parameters.Add("@pk", pkValue);
        parameters.Add("@nowEpoch", nowEpoch ?? NowEpoch());

        if (skParams is not null)
            parameters.AddDynamicParams(skParams);

        var sql = $"""SELECT pk AS Pk, sk AS Sk, table_pk AS TablePk, table_sk AS TableSk, item_json AS ItemJson FROM "{idxTable}" WHERE pk = @pk AND (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)""";

        if (skWhereSql is not null)
            sql += $" AND {skWhereSql}";

        if (exclusiveStartSk is not null)
        {
            var skOp = ascending ? ">" : "<";
            parameters.Add("@esSk", exclusiveStartSk);
            parameters.Add("@esTablePk", exclusiveStartTablePk ?? string.Empty);
            parameters.Add("@esTableSk", exclusiveStartTableSk ?? string.Empty);
            sql += $" AND ({orderByColumn} {skOp} @esSk OR ({orderByColumn} = @esSk AND (table_pk > @esTablePk OR (table_pk = @esTablePk AND table_sk > @esTableSk))))";
        }

        var direction = ascending ? "ASC" : "DESC";
        sql += $" ORDER BY {orderByColumn} {direction}, table_pk {direction}, table_sk {direction}";

        if (limit is not null)
        {
            parameters.Add("@limit", limit.Value);
            sql += " LIMIT @limit";
        }

        return (await connection.QueryAsync<IndexItemRow>(sql, parameters)).AsList();
    }

    internal async Task<List<IndexItemRow>> ScanIndexItemsAsync(
        string tableName,
        string indexName,
        int? limit,
        string? exclusiveStartPk,
        string? exclusiveStartSk,
        string? exclusiveStartTablePk,
        string? exclusiveStartTableSk,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var idxTable = IndexTableName(tableName, indexName);
        var parameters = new DynamicParameters();
        parameters.Add("@nowEpoch", nowEpoch ?? NowEpoch());

        var sql = $"""SELECT pk AS Pk, sk AS Sk, table_pk AS TablePk, table_sk AS TableSk, item_json AS ItemJson FROM "{idxTable}" WHERE (ttl_epoch IS NULL OR ttl_epoch >= @nowEpoch)""";

        if (exclusiveStartPk is not null)
        {
            parameters.Add("@esPk", exclusiveStartPk);
            parameters.Add("@esSk", exclusiveStartSk ?? string.Empty);
            parameters.Add("@esTablePk", exclusiveStartTablePk ?? string.Empty);
            parameters.Add("@esTableSk", exclusiveStartTableSk ?? string.Empty);
            sql += " AND (pk > @esPk OR (pk = @esPk AND sk > @esSk) OR (pk = @esPk AND sk = @esSk AND (table_pk > @esTablePk OR (table_pk = @esTablePk AND table_sk > @esTableSk))))";
        }

        sql += " ORDER BY pk, sk, table_pk, table_sk";

        if (limit is not null)
        {
            parameters.Add("@limit", limit.Value);
            sql += " LIMIT @limit";
        }

        return (await connection.QueryAsync<IndexItemRow>(sql, parameters)).AsList();
    }

    // ── Update table metadata (for UpdateTableAsync GSI changes) ───

    internal async Task UpdateIndexMetadataAsync(
        string tableName,
        List<IndexDefinition> gsiDefinitions,
        CancellationToken cancellationToken = default)
    {
        var gsiJson = SerializeIndexDefinitions(gsiDefinitions);
        using var connection = await OpenConnectionAsync(cancellationToken);
        _ = await connection.ExecuteAsync(
            "UPDATE tables SET global_secondary_indexes_json = @gsiJson WHERE table_name = @tableName",
            new { tableName, gsiJson });
    }

    internal async Task UpdateIndexMetadataInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        List<IndexDefinition> gsiDefinitions,
        List<AttributeDefinition>? updatedAttrDefs = null)
    {
        var gsiJson = SerializeIndexDefinitions(gsiDefinitions);

        if (updatedAttrDefs is not null)
        {
            var attrDefsJson = JsonSerializer.Serialize(updatedAttrDefs.Select(static a =>
                new { a.AttributeName, AttributeType = a.AttributeType.Value }).ToList());
            _ = await connection.ExecuteAsync(
                "UPDATE tables SET global_secondary_indexes_json = @gsiJson, attribute_definitions_json = @attrDefsJson WHERE table_name = @tableName",
                new { tableName, gsiJson, attrDefsJson },
                transaction);
        }
        else
        {
            _ = await connection.ExecuteAsync(
                "UPDATE tables SET global_secondary_indexes_json = @gsiJson WHERE table_name = @tableName",
                new { tableName, gsiJson },
                transaction);
        }
    }

    internal async Task<List<ItemRow>> GetAllItemsAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<ItemRow>(
            "SELECT pk AS Pk, sk AS Sk, item_json AS ItemJson FROM items WHERE table_name = @tableName",
            new { tableName })).AsList();
    }

    // ── Index methods that need connection+transaction (for writes) ─

    internal async Task<(string? OldJson, DbConnection Connection, DbTransaction Transaction)> PutItemWithTransactionAsync(
        string tableName,
        string pk,
        string sk,
        string itemJson,
        double? skNum,
        double? ttlEpoch = null,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await PutItemCoreAsync(connection, transaction, tableName, pk, sk, itemJson, skNum, ttlEpoch, nowEpoch ?? NowEpoch());
        return (oldJson, connection, transaction);
    }

    internal async Task<(string? OldJson, DbConnection Connection, DbTransaction Transaction)> DeleteItemWithTransactionAsync(
        string tableName,
        string pk,
        string sk,
        double? nowEpoch = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await DeleteItemCoreAsync(connection, transaction, tableName, pk, sk, nowEpoch);
        return (oldJson, connection, transaction);
    }

    // ── Index serialization helpers ────────────────────────────────

    [SuppressMessage("IDisposableAnalyzers.Correctness", "IDISP004:Don't ignore created IDisposable", Justification = "ArrayEnumerator is a struct; foreach disposes it")]
    private static List<IndexDefinition> DeserializeIndexDefinitions(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var result = new List<IndexDefinition>();

        foreach (var e in doc.RootElement.EnumerateArray())
        {
            var keySchema = new List<KeySchemaElement>();
            foreach (var k in e.GetProperty("KeySchema").EnumerateArray())
            {
                keySchema.Add(new KeySchemaElement
                {
                    AttributeName = k.GetProperty("AttributeName").GetString()!,
                    KeyType = k.GetProperty("KeyType").GetString()!
                });
            }

            var projectionType = e.GetProperty("ProjectionType").GetString()!;
            List<string>? nonKeyAttrs = null;

            if (e.TryGetProperty("NonKeyAttributes", out var nka) && nka.ValueKind == JsonValueKind.Array)
            {
                nonKeyAttrs = [];
                foreach (var attr in nka.EnumerateArray())
                    nonKeyAttrs.Add(attr.GetString()!);
            }

            result.Add(new IndexDefinition(
                e.GetProperty("IndexName").GetString()!,
                e.TryGetProperty("IsGlobal", out var ig) && ig.GetBoolean(),
                keySchema,
                projectionType,
                nonKeyAttrs));
        }

        return result;
    }

    private static string SerializeIndexDefinitions(List<IndexDefinition> indexes) =>
        JsonSerializer.Serialize(indexes.Select(static idx => new
        {
            idx.IndexName,
            idx.IsGlobal,
            KeySchema = idx.KeySchema.Select(static k => new { k.AttributeName, KeyType = k.KeyType.Value }).ToList(),
            idx.ProjectionType,
            idx.NonKeyAttributes
        }).ToList());

    // ── TTL cleanup & backfill ──────────────────────────────────────

    internal async Task CleanupExpiredItemsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        if (lastCleanupByTable.TryGetValue(tableName, out var lastCleanup)
            && (now - lastCleanup).TotalSeconds < 30)
            return;

        lastCleanupByTable[tableName] = now;

        var nowEpoch = NowEpoch();
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        _ = await connection.ExecuteAsync(
            "DELETE FROM items WHERE table_name = @tableName AND ttl_epoch IS NOT NULL AND ttl_epoch < @nowEpoch",
            new { tableName, nowEpoch },
            transaction);

        var indexes = await GetIndexDefinitionsAsync(connection, tableName, transaction);
        foreach (var idx in indexes)
        {
            var idxTable = IndexTableName(tableName, idx.IndexName);
            _ = await connection.ExecuteAsync(
                $"""DELETE FROM "{idxTable}" WHERE ttl_epoch IS NOT NULL AND ttl_epoch < @nowEpoch""",
                new { nowEpoch },
                transaction);
        }

        _ = await connection.ExecuteAsync(
            """
            UPDATE tables SET
                item_count = (SELECT COUNT(1) FROM items WHERE table_name = @tableName),
                table_size_bytes = (SELECT COALESCE(SUM(LENGTH(item_json)), 0) FROM items WHERE table_name = @tableName)
            WHERE table_name = @tableName
            """,
            new { tableName },
            transaction);

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task BatchUpdateTtlEpochAsync(string tableName, List<(string Pk, string Sk, double? TtlEpoch)> updates, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var (pk, sk, ttlEpoch) in updates)
        {
            _ = await connection.ExecuteAsync(
                "UPDATE items SET ttl_epoch = @ttlEpoch WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
                new { tableName, pk, sk, ttlEpoch },
                transaction);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task ClearTtlEpochAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        _ = await connection.ExecuteAsync(
            "UPDATE items SET ttl_epoch = NULL WHERE table_name = @tableName",
            new { tableName },
            transaction);

        var indexes = await GetIndexDefinitionsAsync(connection, tableName, transaction);
        foreach (var idx in indexes)
        {
            var idxTable = IndexTableName(tableName, idx.IndexName);
            _ = await connection.ExecuteAsync(
                $"""UPDATE "{idxTable}" SET ttl_epoch = NULL""",
                transaction: transaction);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task BackfillIndexTtlEpochAsync(string tableName, string indexName, string ttlAttributeName, CancellationToken cancellationToken = default)
    {
        var idxTable = IndexTableName(tableName, indexName);
        using var connection = await OpenConnectionAsync(cancellationToken);

        var rows = (await connection.QueryAsync<IndexItemRow>(
            $"""SELECT pk AS Pk, sk AS Sk, table_pk AS TablePk, table_sk AS TableSk, item_json AS ItemJson FROM "{idxTable}" """))
            .AsList();

        using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        foreach (var row in rows)
        {
            var item = AttributeValueSerializer.Deserialize(row.ItemJson);
            var ttlEpoch = TtlHelper.ExtractTtlEpoch(item, ttlAttributeName);
            _ = await connection.ExecuteAsync(
                $"""UPDATE "{idxTable}" SET ttl_epoch = @ttlEpoch WHERE pk = @Pk AND sk = @Sk AND table_pk = @TablePk AND table_sk = @TableSk""",
                new { ttlEpoch, row.Pk, row.Sk, row.TablePk, row.TableSk },
                transaction);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public void Dispose()
    {
        if (disposed)
            return;

        sentinel.Dispose();
        disposed = true;
    }
}

internal sealed record BatchWriteOperation(
    string TableName,
    string Pk,
    string Sk,
    double? SkNum,
    double? TtlEpoch,
    string? ItemJson);

internal sealed record TransactWriteOperation(
    string TableName,
    string Pk,
    string Sk,
    double? SkNum,
    double? TtlEpoch,
    string? ItemJson,
    bool IsDelete);

internal sealed record TableRow(
    string TableName,
    string KeySchemaJson,
    string AttributeDefinitionsJson,
    string ProvisionedThroughputJson,
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson,
    string CreatedAt,
    string Status,
    long ItemCount,
    long TableSizeBytes);

internal sealed record IndexMetadataRow(
    string GlobalSecondaryIndexesJson,
    string LocalSecondaryIndexesJson);

internal sealed record KeySchemaRow(
    string KeySchemaJson,
    string AttributeDefinitionsJson);

internal sealed record KeySchemaInfo(
    List<KeySchemaElement> KeySchema,
    List<AttributeDefinition> AttributeDefinitions);

internal sealed record ItemRow(string Pk, string Sk, string ItemJson);

internal sealed record IndexItemRow(string Pk, string Sk, string TablePk, string TableSk, string ItemJson);

internal sealed record IndexDefinition(
    string IndexName,
    bool IsGlobal,
    List<KeySchemaElement> KeySchema,
    string ProjectionType,
    List<string>? NonKeyAttributes);
