using System.Reflection;
using Xunit.Sdk;
using Xunit.v3;

namespace DynamoDbLite.Parity.Tests.Fixtures;

// Emits one row per ParityBackend and tags each row with a Backend trait so
// `dotnet test --filter "Trait=Backend=DynamoDbLocal"` (or DdbLite / DdbLiteFile)
// selects a single backend across the entire parity suite.
public sealed class BackendDataAttribute : DataAttribute
{
    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var rows = new ITheoryDataRow[]
        {
            Row(ParityBackend.DdbLite),
            Row(ParityBackend.DdbLiteFile),
            Row(ParityBackend.DynamoDbLocal),
        };
        return new(rows);
    }

    public override bool SupportsDiscoveryEnumeration() => true;

    private static TheoryDataRow<ParityBackend> Row(ParityBackend backend)
    {
        var row = new TheoryDataRow<ParityBackend>(backend);
        row.Traits.Add("Backend", [backend.ToString()]);
        return row;
    }
}
