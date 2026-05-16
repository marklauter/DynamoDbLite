// Out of scope per ADR 0006. See docs/adrs/0006-out-of-scope-operations.md.
// Every method in this file throws NotSupportedException by design and is excluded
// from coverage — there is no behavior to exercise, and asserting "throws" on each
// would add ceremony without signal.
using Amazon.DynamoDBv2.Model;
using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite;

public sealed partial class DynamoDbClient
{
    #region Backups

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion

    #region Global tables

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion

    #region Kinesis streams

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion

    #region PartiQL

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion

    #region Contributor insights

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion

    #region Resource policies

    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    /// <summary>Not supported.</summary>
    [ExcludeFromCodeCoverage]
    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    #endregion
}
