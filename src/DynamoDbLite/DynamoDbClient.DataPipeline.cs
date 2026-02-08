using Amazon.DynamoDBv2.Model;

namespace DynamoDbLite;

// ── Export & Import (Not Yet Implemented) ────────────────────────────
public sealed partial class DynamoDbClient
{
    public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
}
