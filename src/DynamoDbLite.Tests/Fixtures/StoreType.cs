using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite.Tests.Fixtures;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "required for test")]
public enum StoreType
{
    FileBased,
    MemoryBased,
}
