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
            .ResideInNamespaceMatching(@"^DynamoDbLite\.(SqliteStores|SqliteStores\.Models|Expressions|Serialization|Paginators)$")
            .And()
            .DoNotHaveNameContaining("<")
            .Should()
            .NotBePublic()
            .Because("The SQLite layout, expression AST/parsers, serialization wire records, and paginator implementations are intentionally not part of the public API; leaking them would lock the package into the current internals. Callers reach the paginators through the AWS SDK's IDynamoDBv2PaginatorFactory interfaces only."));

    // The namespace rule above only binds types that land in DynamoDbLite.Paginators, so a public
    // paginator in any other namespace slips past it. This one is keyed on the type surface: wherever
    // a paginator lives, it must not be public.
    //
    // Keyed on the name rather than on the implemented interface deliberately. The obvious form,
    // AreAssignableTo(Interfaces().That().HaveFullNameMatching("Amazon.DynamoDBv2.Model.I*Paginator")),
    // compiles and is permanently dead: only DynamoDbLite.dll is loaded into the architecture, and
    // probing that form against Amazon.DynamoDBv2.IAmazonDynamoDB — which DynamoDbClient does
    // implement, publicly — matched zero types. Do not "strengthen" this rule back into that shape
    // without re-running that probe.
    //
    // WithoutRequiringPositiveResults because this is a forward guard: ArchUnitNET fails a rule whose
    // predicate matches nothing, and no paginator types exist yet. It binds the moment one does.
    [Fact]
    public void PaginatorImplementationsAreNotPublic() =>
        Verify(Types()
            .That()
            .HaveNameContaining("Paginator")
            .And()
            .DoNotHaveNameContaining("<")
            .Should()
            .NotBePublic()
            .Because("Paginators are reached through the AWS SDK's IDynamoDBv2PaginatorFactory interfaces; a public concrete paginator would put our paging internals in the API surface.")
            .WithoutRequiringPositiveResults());

    private static void Verify(IArchRule rule)
    {
        if (!rule.HasNoViolations(DynamoDbLite))
        {
            Assert.Fail(rule.Evaluate(DynamoDbLite).ToErrorMessage());
        }
    }
}
