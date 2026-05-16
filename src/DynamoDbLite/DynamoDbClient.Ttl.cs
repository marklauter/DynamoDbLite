using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.SqliteStores;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    /// <inheritdoc/>
    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) =>
        DescribeTimeToLiveAsync(new DescribeTimeToLiveRequest { TableName = tableName }, cancellationToken);

    /// <inheritdoc/>
    public async Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        if (!await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var ttlAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);

        return new DescribeTimeToLiveResponse
        {
            TimeToLiveDescription = new TimeToLiveDescription
            {
                TimeToLiveStatus = ttlAttr is not null ? TimeToLiveStatus.ENABLED : TimeToLiveStatus.DISABLED,
                AttributeName = ttlAttr
            },
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    /// <inheritdoc/>
    public async Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.TimeToLiveSpecification);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.TableName);

        if (!await store.TableExistsAsync(request.TableName, cancellationToken))
            throw new ResourceNotFoundException($"Requested resource not found: Table: {request.TableName} not found");

        var currentAttr = await store.GetTtlAttributeNameAsync(request.TableName, cancellationToken);
        var spec = request.TimeToLiveSpecification;

        if (spec.Enabled is not true and not false)
            throw new AmazonDynamoDBException(
                "One or more parameter values were invalid: TimeToLiveSpecification.Enabled must be specified");

        if (spec.Enabled is true)
        {
            if (currentAttr is not null)
                throw new AmazonDynamoDBException(
                    "TimeToLive is already enabled on the table");

            ArgumentException.ThrowIfNullOrWhiteSpace(spec.AttributeName);
            await store.SetTtlConfigAsync(request.TableName, spec.AttributeName, cancellationToken);
            await BackfillTtlEpochAsync(request.TableName, spec.AttributeName, cancellationToken);
        }
        else
        {
            if (currentAttr is null)
                throw new AmazonDynamoDBException(
                    "TimeToLive is already disabled on the table");

            await store.RemoveTtlConfigAsync(request.TableName, cancellationToken);
            await store.ClearTtlEpochAsync(request.TableName, cancellationToken);
        }

        return new UpdateTimeToLiveResponse
        {
            TimeToLiveSpecification = spec,
            HttpStatusCode = System.Net.HttpStatusCode.OK
        };
    }

    private async Task BackfillTtlEpochAsync(string tableName, string ttlAttributeName, CancellationToken cancellationToken)
    {
        var items = await store.GetAllItemsAsync(tableName, cancellationToken);
        var updates = new List<(string Pk, string Sk, double? TtlEpoch)>(items.Count);
        foreach (var row in items)
        {
            var item = AttributeValueSerializer.Deserialize(row.ItemJson);
            double? ttlEpoch = TtlEpochParser.TryParse(item, ttlAttributeName, out var epoch) ? epoch : null;
            updates.Add((row.Pk, row.Sk, ttlEpoch));
        }

        await store.BatchUpdateTtlEpochAsync(tableName, updates, cancellationToken);

        var indexes = await store.GetIndexDefinitionsAsync(tableName, cancellationToken);
        foreach (var idx in indexes)
            await store.BackfillIndexTtlEpochAsync(tableName, idx.IndexName, ttlAttributeName, cancellationToken);
    }
}
