using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Dapper;
using Microsoft.Data.Sqlite;
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
                item_json   TEXT NOT NULL,
                PRIMARY KEY (table_name, pk, sk)
            );
            """);
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

    internal async Task<string?> PutItemAsync(string tableName, string pk, string sk, string itemJson, double? skNum = null, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await PutItemCoreAsync(connection, transaction, tableName, pk, sk, itemJson, skNum);
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
        double? skNum)
    {
        var oldJson = await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
            new { tableName, pk, sk },
            transaction);

        _ = await connection.ExecuteAsync(
            """
            INSERT INTO items (table_name, pk, sk, sk_num, item_json)
            VALUES (@tableName, @pk, @sk, @skNum, @itemJson)
            ON CONFLICT (table_name, pk, sk) DO UPDATE SET item_json = @itemJson, sk_num = @skNum
            """,
            new { tableName, pk, sk, skNum, itemJson },
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

    internal async Task<string?> GetItemAsync(string tableName, string pk, string sk, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
            new { tableName, pk, sk });
    }

    internal async Task<string?> DeleteItemAsync(string tableName, string pk, string sk, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await DeleteItemCoreAsync(connection, transaction, tableName, pk, sk);
        await transaction.CommitAsync(cancellationToken);
        return oldJson;
    }

    private static async Task<string?> DeleteItemCoreAsync(
        DbConnection connection,
        DbTransaction transaction,
        string tableName,
        string pk,
        string sk)
    {
        var oldJson = await connection.QuerySingleOrDefaultAsync<string>(
            "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
            new { tableName, pk, sk },
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
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var parameters = new DynamicParameters();
        parameters.Add("@tableName", tableName);
        parameters.Add("@pk", pkValue);

        if (skParams is not null)
            parameters.AddDynamicParams(skParams);

        var sql = "SELECT pk AS Pk, sk AS Sk, item_json AS ItemJson FROM items WHERE table_name = @tableName AND pk = @pk";

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
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var parameters = new DynamicParameters();
        parameters.Add("@tableName", tableName);

        var sql = "SELECT pk AS Pk, sk AS Sk, item_json AS ItemJson FROM items WHERE table_name = @tableName";

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
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var results = new List<(string TableName, string ItemJson)>(keys.Count);

        foreach (var (tableName, pk, sk) in keys)
        {
            var itemJson = await connection.QuerySingleOrDefaultAsync<string>(
                "SELECT item_json FROM items WHERE table_name = @tableName AND pk = @pk AND sk = @sk",
                new { tableName, pk, sk });

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
                    INSERT INTO items (table_name, pk, sk, sk_num, item_json)
                    VALUES (@TableName, @Pk, @Sk, @SkNum, @ItemJson)
                    ON CONFLICT (table_name, pk, sk) DO UPDATE SET item_json = @ItemJson, sk_num = @SkNum
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
                    info.Indexes, info.AttrDefs, newItem, oldJson);
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
        string itemJson)
    {
        var idxTable = IndexTableName(tableName, indexName);
        _ = await connection.ExecuteAsync(
            $"""
            INSERT INTO "{idxTable}" (pk, sk, sk_num, table_pk, table_sk, item_json)
            VALUES (@indexPk, @indexSk, @indexSkNum, @tablePk, @tableSk, @itemJson)
            ON CONFLICT (pk, sk, table_pk, table_sk) DO UPDATE SET item_json = @itemJson, sk_num = @indexSkNum
            """,
            new { indexPk, indexSk, indexSkNum, tablePk, tableSk, itemJson },
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
        string? oldItemJson)
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
                        keys.Value.Pk, keys.Value.Sk, skNum, tablePk, tableSk, itemJson);
                }
            }
        }
    }

    private static double? ComputeIndexSkNum(
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
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var idxTable = IndexTableName(tableName, indexName);
        var parameters = new DynamicParameters();
        parameters.Add("@pk", pkValue);

        if (skParams is not null)
            parameters.AddDynamicParams(skParams);

        var sql = $"""SELECT pk AS Pk, sk AS Sk, table_pk AS TablePk, table_sk AS TableSk, item_json AS ItemJson FROM "{idxTable}" WHERE pk = @pk""";

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
        CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        var idxTable = IndexTableName(tableName, indexName);
        var parameters = new DynamicParameters();

        var sql = $"""SELECT pk AS Pk, sk AS Sk, table_pk AS TablePk, table_sk AS TableSk, item_json AS ItemJson FROM "{idxTable}" """;

        if (exclusiveStartPk is not null)
        {
            parameters.Add("@esPk", exclusiveStartPk);
            parameters.Add("@esSk", exclusiveStartSk ?? string.Empty);
            parameters.Add("@esTablePk", exclusiveStartTablePk ?? string.Empty);
            parameters.Add("@esTableSk", exclusiveStartTableSk ?? string.Empty);
            sql += " WHERE (pk > @esPk OR (pk = @esPk AND sk > @esSk) OR (pk = @esPk AND sk = @esSk AND (table_pk > @esTablePk OR (table_pk = @esTablePk AND table_sk > @esTableSk))))";
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
        CancellationToken cancellationToken = default)
    {
        var connection = await OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await PutItemCoreAsync(connection, transaction, tableName, pk, sk, itemJson, skNum);
        return (oldJson, connection, transaction);
    }

    internal async Task<(string? OldJson, DbConnection Connection, DbTransaction Transaction)> DeleteItemWithTransactionAsync(
        string tableName,
        string pk,
        string sk,
        CancellationToken cancellationToken = default)
    {
        var connection = await OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var oldJson = await DeleteItemCoreAsync(connection, transaction, tableName, pk, sk);
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
    string? ItemJson);

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
