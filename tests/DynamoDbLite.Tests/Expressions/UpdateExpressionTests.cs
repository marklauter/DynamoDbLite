using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDbLite.Expressions;

namespace DynamoDbLite.Tests.Expressions;

public sealed class UpdateExpressionTests
{
    // ── SET ─────────────────────────────────────────────────────────────

    [Fact]
    public void Set_SimpleValue_SetsAttribute()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET #n = :val");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "Bob" } });

        Assert.Equal("Bob", result["name"].S);
        Assert.Contains("name", modifiedKeys);
    }

    [Fact]
    public void Set_MultipleAttributes_SetsAll()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET #n = :name, age = :age");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue>
            {
                [":name"] = new() { S = "Charlie" },
                [":age"] = new() { N = "25" }
            });

        Assert.Equal("Charlie", result["name"].S);
        Assert.Equal("25", result["age"].N);
    }

    [Fact]
    public void Set_ArithmeticAdd_IncrementsNumber()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = age + :inc");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "5" } });

        Assert.Equal("35", result["age"].N);
    }

    [Fact]
    public void Set_ArithmeticSubtract_DecrementsNumber()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = age - :dec");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":dec"] = new() { N = "5" } });

        Assert.Equal("25", result["age"].N);
    }

    [Fact]
    public void Set_IfNotExists_UsesDefault_WhenNotPresent()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET score = if_not_exists(score, :default)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":default"] = new() { N = "0" } });

        Assert.Equal("0", result["score"].N);
    }

    [Fact]
    public void Set_IfNotExists_UsesExisting_WhenPresent()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = if_not_exists(age, :default)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":default"] = new() { N = "0" } });

        Assert.Equal("30", result["age"].N);
    }

    [Fact]
    public void Set_ListAppend_ConcatenatesLists()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }] }
        };

        var ast = UpdateExpressionParser.Parse("SET entries = list_append(entries, :newItems)");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":newItems"] = new() { L = [new() { S = "b" }, new() { S = "c" }] }
            });

        Assert.Equal(3, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].S);
        Assert.Equal("b", result["entries"].L[1].S);
        Assert.Equal("c", result["entries"].L[2].S);
    }

    // ── REMOVE ─────────────────────────────────────────────────────────

    [Fact]
    public void Remove_ExistingAttribute_RemovesIt()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("REMOVE #n");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" }, null);

        Assert.False(result.ContainsKey("name"));
        Assert.Contains("name", modifiedKeys);
    }

    [Fact]
    public void Remove_MultipleAttributes_RemovesAll()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("REMOVE #n, age");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" }, null);

        Assert.False(result.ContainsKey("name"));
        Assert.False(result.ContainsKey("age"));
    }

    // ── ADD ─────────────────────────────────────────────────────────────

    [Fact]
    public void Add_Number_IncrementsExistingValue()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("ADD age :inc");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":inc"] = new() { N = "10" } });

        Assert.Equal("40", result["age"].N);
    }

    [Fact]
    public void Add_Number_CreatesIfNotExists()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("ADD score :val");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { N = "100" } });

        Assert.Equal("100", result["score"].N);
    }

    [Fact]
    public void Add_StringSet_UnionsElements()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["tags"] = new() { SS = ["a", "b"] }
        };

        var ast = UpdateExpressionParser.Parse("ADD tags :newTags");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":newTags"] = new() { SS = ["b", "c"] }
            });

        Assert.Equal(3, result["tags"].SS.Count);
        Assert.Contains("a", result["tags"].SS);
        Assert.Contains("b", result["tags"].SS);
        Assert.Contains("c", result["tags"].SS);
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    [Fact]
    public void Delete_StringSet_RemovesElements()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["tags"] = new() { SS = ["a", "b", "c"] }
        };

        var ast = UpdateExpressionParser.Parse("DELETE tags :removeTags");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":removeTags"] = new() { SS = ["b"] }
            });

        Assert.Equal(2, result["tags"].SS.Count);
        Assert.Contains("a", result["tags"].SS);
        Assert.Contains("c", result["tags"].SS);
    }

    // ── Empty container path resolution ────────────────────────────────

    [Fact]
    public void Set_ThroughEmptyMap_SetsValue()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["a"] = new() { M = [] },
        };

        var ast = UpdateExpressionParser.Parse("SET a.b = :val");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":val"] = new() { S = "hello" } });

        Assert.Equal("hello", result["a"].M["b"].S);
    }

    [Fact]
    public void Remove_ThroughEmptyMap_NoOp()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["a"] = new() { M = [] },
        };

        var ast = UpdateExpressionParser.Parse("REMOVE a.b");
        var (result, _) = UpdateExpressionEvaluator.Apply(ast, item, null, null);

        Assert.Empty(result["a"].M);
    }

    // ── list_append validation ──────────────────────────────────────────

    [Fact]
    public void Set_ListAppend_NonListFirstOperand_Throws()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }] },
        };

        var ast = UpdateExpressionParser.Parse("SET entries = list_append(:val, entries)");
        var ex = Assert.Throws<ArgumentException>(() =>
            UpdateExpressionEvaluator.Apply(
                ast, item, null,
                new Dictionary<string, AttributeValue> { [":val"] = new() { S = "not-a-list" } }));

        Assert.Contains("list_append", ex.Message);
    }

    [Fact]
    public void Set_ListAppend_NonListSecondOperand_Throws()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }] },
        };

        var ast = UpdateExpressionParser.Parse("SET entries = list_append(entries, :val)");
        var ex = Assert.Throws<ArgumentException>(() =>
            UpdateExpressionEvaluator.Apply(
                ast, item, null,
                new Dictionary<string, AttributeValue> { [":val"] = new() { N = "42" } }));

        Assert.Contains("list_append", ex.Message);
    }

    [Fact]
    public void Set_ListAppend_BothNonList_Throws()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
        };

        var ast = UpdateExpressionParser.Parse("SET entries = list_append(:a, :b)");
        var ex = Assert.Throws<ArgumentException>(() =>
            UpdateExpressionEvaluator.Apply(
                ast, item, null,
                new Dictionary<string, AttributeValue>
                {
                    [":a"] = new() { S = "not-a-list" },
                    [":b"] = new() { N = "42" },
                }));

        Assert.Contains("list_append", ex.Message);
    }

    // ── Combined clauses ───────────────────────────────────────────────

    [Fact]
    public void SetAndRemove_Combined_AppliesBoth()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("SET age = :age REMOVE #n");
        var (result, modifiedKeys) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#n"] = "name" },
            new Dictionary<string, AttributeValue> { [":age"] = new() { N = "31" } });

        Assert.Equal("31", result["age"].N);
        Assert.False(result.ContainsKey("name"));
        Assert.Contains("age", modifiedKeys);
        Assert.Contains("name", modifiedKeys);
    }

    // ── ADD on number/binary sets ──────────────────────────────────────

    [Fact]
    public void Add_NumberSet_UnionsValues()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["nums"] = new() { NS = ["1", "2"] }
        };

        var ast = UpdateExpressionParser.Parse("ADD nums :more");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":more"] = new() { NS = ["2", "3"] } });

        Assert.Equal(3, result["nums"].NS.Count);
        Assert.Contains("1", result["nums"].NS);
        Assert.Contains("2", result["nums"].NS);
        Assert.Contains("3", result["nums"].NS);
    }

    [Fact]
    public void Add_BinarySet_UnionsValues_DedupedByContent()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["blobs"] = new() { BS = [new MemoryStream([0x01]), new MemoryStream([0x02])] }
        };

        var ast = UpdateExpressionParser.Parse("ADD blobs :more");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue>
            {
                [":more"] = new() { BS = [new MemoryStream([0x02]), new MemoryStream([0x03])] }
            });

        Assert.Equal(3, result["blobs"].BS.Count);
        Assert.Contains(result["blobs"].BS, b => b.ToArray().SequenceEqual([(byte)0x01]));
        Assert.Contains(result["blobs"].BS, b => b.ToArray().SequenceEqual([(byte)0x02]));
        Assert.Contains(result["blobs"].BS, b => b.ToArray().SequenceEqual([(byte)0x03]));
    }

    // ── DELETE on number/binary sets ───────────────────────────────────

    [Fact]
    public void Delete_NumberSet_RemovesValues()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["nums"] = new() { NS = ["1", "2", "3"] }
        };

        var ast = UpdateExpressionParser.Parse("DELETE nums :rm");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":rm"] = new() { NS = ["2"] } });

        Assert.Equal(2, result["nums"].NS.Count);
        Assert.Contains("1", result["nums"].NS);
        Assert.Contains("3", result["nums"].NS);
    }

    [Fact]
    public void Delete_BinarySet_RemovesValues_ByContent()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["blobs"] = new() { BS = [new MemoryStream([0x01]), new MemoryStream([0x02]), new MemoryStream([0x03])] }
        };

        var ast = UpdateExpressionParser.Parse("DELETE blobs :rm");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":rm"] = new() { BS = [new MemoryStream([0x02])] } });

        Assert.Equal(2, result["blobs"].BS.Count);
        Assert.Contains(result["blobs"].BS, b => b.ToArray().SequenceEqual([(byte)0x01]));
        Assert.Contains(result["blobs"].BS, b => b.ToArray().SequenceEqual([(byte)0x03]));
    }

    [Fact]
    public void Delete_OnMissingPath_NoOp()
    {
        var item = CreateTestItem();
        var ast = UpdateExpressionParser.Parse("DELETE absent :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { SS = ["x"] } });

        Assert.False(result.ContainsKey("absent"));
        Assert.Equal("Alice", result["name"].S);
    }

    // ── List-index path support ────────────────────────────────────────

    [Fact]
    public void Set_ListIndex_AssignsAtIndex()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }, new() { S = "b" }, new() { S = "c" }] }
        };

        var ast = UpdateExpressionParser.Parse("SET entries[1] = :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "B" } });

        Assert.Equal(3, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].S);
        Assert.Equal("B", result["entries"].L[1].S);
        Assert.Equal("c", result["entries"].L[2].S);
    }

    [Fact]
    public void Set_ListIndex_AutoExtends_WithNullPlaceholders()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }] }
        };

        var ast = UpdateExpressionParser.Parse("SET entries[3] = :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "X" } });

        Assert.Equal(4, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].S);
        Assert.True(result["entries"].L[1].NULL);
        Assert.True(result["entries"].L[2].NULL);
        Assert.Equal("X", result["entries"].L[3].S);
    }

    [Fact]
    public void Remove_ListIndex_RemovesElement()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["entries"] = new() { L = [new() { S = "a" }, new() { S = "b" }, new() { S = "c" }] }
        };

        var ast = UpdateExpressionParser.Parse("REMOVE entries[1]");
        var (result, _) = UpdateExpressionEvaluator.Apply(ast, item, null, null);

        Assert.Equal(2, result["entries"].L.Count);
        Assert.Equal("a", result["entries"].L[0].S);
        Assert.Equal("c", result["entries"].L[1].S);
    }

    [Fact]
    public void Set_NestedListIndex_AssignsAtIndex()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["nested"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["child"] = new() { L = [new() { S = "a" }, new() { S = "b" }] }
                }
            }
        };

        var ast = UpdateExpressionParser.Parse("SET nested.child[0] = :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "A" } });

        Assert.Equal("A", result["nested"].M["child"].L[0].S);
        Assert.Equal("b", result["nested"].M["child"].L[1].S);
    }

    [Fact]
    public void Remove_NestedListIndex_RemovesElement()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" },
            ["nested"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["child"] = new() { L = [new() { S = "a" }, new() { S = "b" }, new() { S = "c" }] }
                }
            }
        };

        var ast = UpdateExpressionParser.Parse("REMOVE nested.child[1]");
        var (result, _) = UpdateExpressionEvaluator.Apply(ast, item, null, null);

        Assert.Equal(2, result["nested"].M["child"].L.Count);
        Assert.Equal("a", result["nested"].M["child"].L[0].S);
        Assert.Equal("c", result["nested"].M["child"].L[1].S);
    }

    [Fact]
    public void Set_NestedMapPath_CreatesIntermediateMaps()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = "USER#1" }
        };

        var ast = UpdateExpressionParser.Parse("SET nested.deep.field = :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "found" } });

        Assert.True(result.ContainsKey("nested"));
        Assert.NotNull(result["nested"].M);
        Assert.True(result["nested"].M.ContainsKey("deep"));
        Assert.Equal("found", result["nested"].M["deep"].M["field"].S);
    }

    // ── Reserved-word rejection ────────────────────────────────────────

    [Fact]
    public void Reserved_TopLevelIdentifier_Throws()
    {
        var ex = Assert.Throws<AmazonDynamoDBException>(() =>
            UpdateExpressionParser.Parse("SET name = :v"));

        Assert.Contains("UpdateExpression", ex.Message);
        Assert.Contains("reserved keyword", ex.Message);
        Assert.Contains("name", ex.Message);
    }

    [Fact]
    public void Reserved_NestedPathElement_Throws() =>
        Assert.Throws<AmazonDynamoDBException>(() =>
            UpdateExpressionParser.Parse("SET a.status = :v"));

    [Fact]
    public void Reserved_CaseInsensitive_Throws() =>
        Assert.Throws<AmazonDynamoDBException>(() =>
            UpdateExpressionParser.Parse("SET NaMe = :v"));

    [Fact]
    public void Reserved_EscapedViaExpressionAttributeName_Allowed()
    {
        var ast = UpdateExpressionParser.Parse("SET #n = :v");

        _ = Assert.Single(ast.Sets);
    }

    [Fact]
    public void Set_IntermediateListIndex_ThenAttribute_UpdatesField()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["a"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["b"] = new()
                    {
                        L =
                        [
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "old0" } } },
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "old1" } } },
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "old2" } } }
                        ]
                    }
                }
            }
        };

        var ast = UpdateExpressionParser.Parse("SET a.b[2].c = :v");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item, null,
            new Dictionary<string, AttributeValue> { [":v"] = new() { S = "new2" } });

        Assert.Equal("new2", result["a"].M["b"].L[2].M["c"].S);
        Assert.Equal("old0", result["a"].M["b"].L[0].M["c"].S);
        Assert.Equal("old1", result["a"].M["b"].L[1].M["c"].S);
    }

    [Fact]
    public void Remove_IntermediateListIndex_ThenAttribute_RemovesField()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["a"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["b"] = new()
                    {
                        L =
                        [
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "v0" } } },
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "v1" } } }
                        ]
                    }
                }
            }
        };

        var ast = UpdateExpressionParser.Parse("REMOVE a.b[1].c");
        var (result, _) = UpdateExpressionEvaluator.Apply(ast, item, null, null);

        Assert.False(result["a"].M["b"].L[1].M.ContainsKey("c"));
        Assert.Equal("v0", result["a"].M["b"].L[0].M["c"].S);
    }

    [Fact]
    public void Remove_IntermediateListIndexOutOfRange_NoOps()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["a"] = new()
            {
                M = new Dictionary<string, AttributeValue>
                {
                    ["b"] = new()
                    {
                        L =
                        [
                            new() { M = new Dictionary<string, AttributeValue> { ["c"] = new() { S = "v0" } } }
                        ]
                    }
                }
            }
        };

        var ast = UpdateExpressionParser.Parse("REMOVE a.b[10].c");
        var (result, _) = UpdateExpressionEvaluator.Apply(ast, item, null, null);

        // List unchanged.
        _ = Assert.Single(result["a"].M["b"].L);
        Assert.Equal("v0", result["a"].M["b"].L[0].M["c"].S);
    }

    [Fact]
    public void Remove_MissingIntermediateAttribute_NoOps()
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["a"] = new() { S = "present" }
        };

        var ast = UpdateExpressionParser.Parse("REMOVE #m.x");
        var (result, _) = UpdateExpressionEvaluator.Apply(
            ast, item,
            new Dictionary<string, string> { ["#m"] = "absent" },
            null);

        Assert.Equal("present", result["a"].S);
        Assert.False(result.ContainsKey("absent"));
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static Dictionary<string, AttributeValue> CreateTestItem() =>
        new()
        {
            ["PK"] = new() { S = "USER#1" },
            ["name"] = new() { S = "Alice" },
            ["age"] = new() { N = "30" },
        };
}
