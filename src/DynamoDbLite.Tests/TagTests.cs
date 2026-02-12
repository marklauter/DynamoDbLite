using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Tests.Fixtures;

namespace DynamoDbLite.Tests;

public sealed class TagTests
    : DynamoDbClientFixture
{
    private const string TableArn = "arn:aws:dynamodb:us-east-1:000000000000:table/TestTable";

    protected override async ValueTask SetupAsync(CancellationToken ct)
    {
        await CreateTestTableAsync(Client(StoreType.MemoryBased), ct);
        await CreateTestTableAsync(Client(StoreType.FileBased), ct);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Adds_Tags_To_Table(StoreType st)
    {
        var client = Client(st);
        var response = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags = [new Tag { Key = "env", Value = "test" }]
        }, TestContext.Current.CancellationToken);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.HttpStatusCode);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTagsOfResource_Returns_Empty_For_Untagged_Table(StoreType st)
    {
        var client = Client(st);
        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        Assert.Empty(response.Tags);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTagsOfResource_Returns_Tags_After_Tagging(StoreType st)
    {
        var client = Client(st);
        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "team", Value = "platform" }
            ]
        }, TestContext.Current.CancellationToken);

        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Tags.Count);
        Assert.Contains(response.Tags, t => t.Key == "env" && t.Value == "test");
        Assert.Contains(response.Tags, t => t.Key == "team" && t.Value == "platform");
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UntagResource_Removes_Specified_Keys(StoreType st)
    {
        var client = Client(st);
        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "team", Value = "platform" }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = TableArn,
            TagKeys = ["env"]
        }, TestContext.Current.CancellationToken);

        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Tags);
        Assert.Equal("team", response.Tags[0].Key);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Overwrites_Existing_Tag_Value(StoreType st)
    {
        var client = Client(st);
        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags = [new Tag { Key = "env", Value = "test" }]
        }, TestContext.Current.CancellationToken);

        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags = [new Tag { Key = "env", Value = "prod" }]
        }, TestContext.Current.CancellationToken);

        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        _ = Assert.Single(response.Tags);
        Assert.Equal("prod", response.Tags[0].Value);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Preserves_Existing_Tags_When_Adding_New(StoreType st)
    {
        var client = Client(st);
        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "team", Value = "platform" }
            ]
        }, TestContext.Current.CancellationToken);

        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags = [new Tag { Key = "version", Value = "v1" }]
        }, TestContext.Current.CancellationToken);

        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, response.Tags.Count);
        Assert.Contains(response.Tags, t => t.Key == "env" && t.Value == "test");
        Assert.Contains(response.Tags, t => t.Key == "team" && t.Value == "platform");
        Assert.Contains(response.Tags, t => t.Key == "version" && t.Value == "v1");
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Validates_Max_50_Tags(StoreType st)
    {
        var client = Client(st);
        var tags = Enumerable.Range(1, 51)
            .Select(i => new Tag { Key = $"key{i}", Value = $"val{i}" })
            .ToList();

        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = TableArn,
                Tags = tags
            }, TestContext.Current.CancellationToken));
        Assert.Contains("Too many tags", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Validates_Key_Length(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = TableArn,
                Tags = [new Tag { Key = new string('k', 129), Value = "v" }]
            }, TestContext.Current.CancellationToken));
        Assert.Contains("Tag key exceeds", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Validates_Value_Length(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<AmazonDynamoDBException>(() =>
            client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = TableArn,
                Tags = [new Tag { Key = "k", Value = new string('v', 257) }]
            }, TestContext.Current.CancellationToken));
        Assert.Contains("Tag value exceeds", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task TagResource_Throws_For_Nonexistent_Table(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = "arn:aws:dynamodb:us-east-1:000000000000:table/NoSuchTable",
                Tags = [new Tag { Key = "k", Value = "v" }]
            }, TestContext.Current.CancellationToken));
        Assert.Contains("NoSuchTable", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task UntagResource_Throws_For_Nonexistent_Table(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = "arn:aws:dynamodb:us-east-1:000000000000:table/NoSuchTable",
                TagKeys = ["k"]
            }, TestContext.Current.CancellationToken));
        Assert.Contains("NoSuchTable", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task ListTagsOfResource_Throws_For_Nonexistent_Table(StoreType st)
    {
        var client = Client(st);
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
            {
                ResourceArn = "arn:aws:dynamodb:us-east-1:000000000000:table/NoSuchTable"
            }, TestContext.Current.CancellationToken));
        Assert.Contains("NoSuchTable", ex.Message);
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task CreateTable_Persists_Tags(StoreType st)
    {
        var client = Client(st);
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TaggedTable",
            KeySchema =
                [new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH }],
            AttributeDefinitions =
                [new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S }],
            Tags =
            [
                new Tag { Key = "env", Value = "staging" },
                new Tag { Key = "cost-center", Value = "123" }
            ]
        }, TestContext.Current.CancellationToken);

        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = "arn:aws:dynamodb:us-east-1:000000000000:table/TaggedTable"
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, response.Tags.Count);
        Assert.Contains(response.Tags, t => t.Key == "env" && t.Value == "staging");
        Assert.Contains(response.Tags, t => t.Key == "cost-center" && t.Value == "123");
    }

    [Theory]
    [InlineData(StoreType.FileBased)]
    [InlineData(StoreType.MemoryBased)]
    public async Task DeleteTable_Removes_Tags(StoreType st)
    {
        var client = Client(st);
        _ = await client.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = TableArn,
            Tags = [new Tag { Key = "env", Value = "test" }]
        }, TestContext.Current.CancellationToken);

        _ = await client.DeleteTableAsync(new DeleteTableRequest { TableName = "TestTable" },
            TestContext.Current.CancellationToken);

        // Re-create table
        _ = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "TestTable",
            KeySchema =
            [
                new KeySchemaElement { AttributeName = "PK", KeyType = KeyType.HASH },
                new KeySchemaElement { AttributeName = "SK", KeyType = KeyType.RANGE }
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition { AttributeName = "PK", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition { AttributeName = "SK", AttributeType = ScalarAttributeType.S }
            ]
        }, TestContext.Current.CancellationToken);

        // Tags should be gone for the re-created table
        var response = await client.ListTagsOfResourceAsync(new ListTagsOfResourceRequest
        {
            ResourceArn = TableArn
        }, TestContext.Current.CancellationToken);

        Assert.Empty(response.Tags);
    }
}
