namespace DynamoDbLite.SqliteStores.Models;

// Cached per-table metadata. The contained lists are shared out of SqliteStore's metadata cache, so
// treat them as read-only — mutating them would corrupt the cache for every other reader.
internal sealed record TableMetadata(
    KeySchemaInfo KeyInfo,
    string? TtlAttributeName,
    List<IndexDefinition> Indexes);
