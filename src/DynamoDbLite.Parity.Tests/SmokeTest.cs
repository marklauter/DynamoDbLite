using Amazon.DynamoDBv2;
using DynamoDbLite.Parity.Tests.Fixtures;

namespace DynamoDbLite.Parity.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class SmokeTest(DynamoDbFixture fixture)
{
    [Fact]
    public async Task DynamoDbLocal_Container_Starts_And_TestTable_Is_Active()
    {
        var response = await fixture.Client.DescribeTableAsync(
            "TestTable",
            TestContext.Current.CancellationToken);

        Assert.Equal(TableStatus.ACTIVE, response.Table.TableStatus);
    }
}
