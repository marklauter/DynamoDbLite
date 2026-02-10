using System.Diagnostics.CodeAnalysis;

namespace DynamoDbLite.Tests.Fixtures;

[CollectionDefinition("DynamoDbFixtureCollection")]
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "test convention")]
public class DynamoDbFixtureCollection
    : ICollectionFixture<DynamoDbFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}
