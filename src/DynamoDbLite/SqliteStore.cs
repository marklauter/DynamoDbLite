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
                table_name                  TEXT NOT NULL PRIMARY KEY,
                key_schema_json             TEXT NOT NULL,
                attribute_definitions_json   TEXT NOT NULL,
                provisioned_throughput_json  TEXT NOT NULL DEFAULT '{}',
                created_at                  TEXT NOT NULL,
                status                      TEXT NOT NULL DEFAULT 'ACTIVE',
                item_count                  INTEGER NOT NULL DEFAULT 0,
                table_size_bytes            INTEGER NOT NULL DEFAULT 0
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

        using var connection = await OpenConnectionAsync(cancellationToken);
        _ = await connection.ExecuteAsync("""
            INSERT INTO tables (table_name, key_schema_json, attribute_definitions_json, provisioned_throughput_json, created_at, status)
            VALUES (@tableName, @keySchemaJson, @attrDefsJson, @throughputJson, @now, 'ACTIVE')
            """,
            new { tableName, keySchemaJson, attrDefsJson, throughputJson, now });
    }

    internal async Task DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

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
                table_name                  AS TableName,
                key_schema_json             AS KeySchemaJson,
                attribute_definitions_json   AS AttributeDefinitionsJson,
                provisioned_throughput_json  AS ProvisionedThroughputJson,
                created_at                  AS CreatedAt,
                status                      AS Status,
                item_count                  AS ItemCount,
                table_size_bytes            AS TableSizeBytes
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

        await transaction.CommitAsync(cancellationToken);
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

        await transaction.CommitAsync(cancellationToken);
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

        return new TableDescription
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
    }

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

    public void Dispose()
    {
        if (disposed)
            return;

        sentinel.Dispose();
        disposed = true;
    }
}

internal sealed record TableRow(
    string TableName,
    string KeySchemaJson,
    string AttributeDefinitionsJson,
    string ProvisionedThroughputJson,
    string CreatedAt,
    string Status,
    long ItemCount,
    long TableSizeBytes);

internal sealed record KeySchemaRow(
    string KeySchemaJson,
    string AttributeDefinitionsJson);

internal sealed record KeySchemaInfo(
    List<KeySchemaElement> KeySchema,
    List<AttributeDefinition> AttributeDefinitions);

internal sealed record ItemRow(string Pk, string Sk, string ItemJson);
