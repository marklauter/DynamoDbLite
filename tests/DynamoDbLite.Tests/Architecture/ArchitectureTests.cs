using ArchUnitNET.Fluent;
using ArchUnitNET.Fluent.Extensions;
using ArchUnitNET.Loader;
using static ArchUnitNET.Fluent.ArchRuleDefinition;
using ArchitectureModel = ArchUnitNET.Domain.Architecture;

namespace DynamoDbLite.Tests.Architecture;

// Encodes the design invariants from docs/agents/architecture.md and
// docs/agents/csharp-guidance.md so drift trips the build, not code review.
public sealed class ArchitectureTests
{
    private static readonly ArchitectureModel DynamoDbLite = new ArchLoader()
        .LoadAssemblies(typeof(DynamoDbClient).Assembly)
        .Build();

    [Fact]
    public void AllTypesResideInDynamoDbLiteTree() =>
        Verify(Types()
            .That()
            .DoNotHaveNameContaining("<") // exclude compiler-generated closures / async state machines
            .Should()
            .ResideInNamespaceMatching(@"^DynamoDbLite(\..*)?$")
            .Because("Allowed sub-namespaces are Expressions, Serialization, SqliteStores, SqliteStores.Models. New top-level sub-namespaces require explicit design review."));

    [Fact]
    public void ConcreteClassesAreSealed() =>
        Verify(Classes()
            .That()
            .AreNotAbstract() // C# 'static' compiles to 'abstract sealed' — this also excludes static factories
            .And()
            .DoNotHaveNameContaining("<")
            .Should()
            .BeSealed()
            .Because("csharp-guidance.md: seal records and classes by default (enables devirtualization)."));

    [Fact]
    public void InstanceFieldsAreNotPublic() =>
        Verify(FieldMembers()
            .That()
            .AreNotStatic() // const / static readonly may be public; instance state must not be.
            .And()
            .DoNotHaveNameContaining("<") // exclude compiler-generated backing fields
            .And()
            .DoNotHaveName("value__") // exclude the implicit instance field every C# enum compiles to
            .Should()
            .NotBePublic()
            .Because("csharp-guidance.md: immutable-by-default; no public mutable instance state."));

    [Fact]
    public void DynamoDbLiteDoesNotDependOnAspNetCore() =>
        Verify(Types()
            .Should()
            .NotDependOnAnyTypesThat()
            .ResideInNamespaceMatching(@"^Microsoft\.AspNetCore.*")
            .Because("DynamoDbLite is a host-free, in-process library; pulling in ASP.NET Core would defeat its purpose (mobile, lambdas, console apps)."));

    [Fact]
    public void DynamoDbLiteDoesNotDependOnHosting() =>
        Verify(Types()
            .Should()
            .NotDependOnAnyTypesThat()
            .ResideInNamespaceMatching(@"^Microsoft\.Extensions\.Hosting.*")
            .Because("DynamoDbLite targets host-free .NET; the consumer owns the host, not DynamoDbLite."));

    [Fact]
    public void DynamoDbLiteDoesNotDependOnConsole() =>
        Verify(Types()
            .Should()
            // HaveFullName is used instead of NotDependOnAny(typeof(Console)) — the typed overload requires
            // the type to be loaded into the architecture, but we only load DynamoDbLite.dll. The name predicate
            // matches against dependency targets recorded by the loader without needing the BCL assembly.
            .NotDependOnAnyTypesThat()
            .HaveFullName("System.Console")
            .Because("Library code routes through ILogger; direct Console writes leak into hosts that suppress stdout (lambdas, services, mobile)."));

    [Fact]
    public void DynamoDbLiteDoesNotDependOnThread() =>
        Verify(Types()
            .Should()
            .NotDependOnAnyTypesThat()
            .HaveFullName("System.Threading.Thread")
            .Because("The IAmazonDynamoDB surface is async-only; Thread primitives (Sleep, Join, Abort) block the calling thread and break the cancellation contract."));

    [Fact]
    public void InternalNamespacesContainOnlyInternalTypes() =>
        Verify(Types()
            .That()
            .ResideInNamespaceMatching(@"^DynamoDbLite\.(SqliteStores|SqliteStores\.Models|Expressions)$")
            .And()
            .DoNotHaveNameContaining("<")
            .Should()
            .NotBePublic()
            .Because("The SQLite layout and the expression AST/parsers are intentionally not part of the public API; leaking them would lock the package into the current internals."));

    private static void Verify(IArchRule rule)
    {
        if (!rule.HasNoViolations(DynamoDbLite))
        {
            Assert.Fail(rule.Evaluate(DynamoDbLite).ToErrorMessage());
        }
    }
}
