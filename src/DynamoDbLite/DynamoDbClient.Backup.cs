using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite;

// ── Backup & Restore (Not Yet Implemented) ──────────────────────────
public sealed partial class DynamoDbClient
{
    public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
}
