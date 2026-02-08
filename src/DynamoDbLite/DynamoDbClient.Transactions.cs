using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite;

// ── Transactions & PartiQL (Not Yet Implemented) ────────────────────
public sealed partial class DynamoDbClient
{
    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
}
