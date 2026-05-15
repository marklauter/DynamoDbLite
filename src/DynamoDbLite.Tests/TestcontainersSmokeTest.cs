using Amazon.DynamoDBv2;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

[Collection("DynamoDbFixtureCollection")]
public sealed class TestcontainersSmokeTest(DynamoDbFixture fixture)
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
