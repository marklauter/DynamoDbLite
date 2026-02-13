using Amazon.DynamoDBv2.Model;
using System.Text.Json;
using StoreModels = DynamoDbLite.SqliteStores.Models;

namespace DynamoDbLite.Serialization;

internal static class SerializerExtensions
{
    internal static string ToJson(this TableCreationParameters p)
    {
        var keySchema = p.KeySchema.ToKeySchemas();

        var attrDefs = new AttributeDefinitionWire[p.AttributeDefinitions.Count];
        for (var i = 0; i < attrDefs.Length; i++)
        {
            var a = p.AttributeDefinitions[i];
            attrDefs[i] = new AttributeDefinitionWire(a.AttributeName, a.AttributeType.Value);
        }

        var throughput = p.ProvisionedThroughput is not null
            ? new ProvisionedThroughputWire(p.ProvisionedThroughput.ReadCapacityUnits, p.ProvisionedThroughput.WriteCapacityUnits)
            : null;

        GsiWire[]? gsis = null;
        if (p.GlobalSecondaryIndexes is { Count: > 0 })
        {
            gsis = new GsiWire[p.GlobalSecondaryIndexes.Count];
            for (var i = 0; i < gsis.Length; i++)
            {
                var g = p.GlobalSecondaryIndexes[i];
                var projection = g.Projection is not null
                    ? new ProjectionWire(g.Projection.ProjectionType?.Value, g.Projection.NonKeyAttributes)
                    : null;
                gsis[i] = new GsiWire(g.IndexName, g.KeySchema.ToKeySchemas(), projection);
            }
        }

        return JsonSerializer.Serialize(new TableCreationWire(p.TableName, keySchema, attrDefs, throughput, gsis));
    }

    internal static string ToJson(this List<StoreModels.IndexDefinition> indexes)
    {
        var models = new IndexDefinitionWire[indexes.Count];
        for (var i = 0; i < models.Length; i++)
        {
            var idx = indexes[i];
            models[i] = new IndexDefinitionWire(idx.IndexName, idx.IsGlobal, idx.KeySchema.ToKeySchemas(), idx.ProjectionType, idx.NonKeyAttributes);
        }

        return JsonSerializer.Serialize(models);
    }
}
