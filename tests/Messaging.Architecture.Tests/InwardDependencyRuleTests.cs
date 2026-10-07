using System.Linq;
using System.Reflection;
using Messaging.Abstractions;
using Messaging.Publishing;

namespace Messaging.Architecture.Tests;

/// <summary>
/// Architecture tests that enforce the Clean Architecture inward-dependency rule (Req 3.1, 3.8):
/// the Domain (<c>Messaging.Abstractions</c>) and Application (<c>Messaging</c>) layers must not
/// take a dependency on any concrete <c>Messaging_Provider</c> implementation. The check inspects
/// the referenced-assembly metadata of each inward assembly rather than relying on a direct
/// compile-time reference, so a forbidden reference is caught even if the test project itself
/// never references the provider packages.
/// </summary>
public sealed class InwardDependencyRuleTests
{
    /// <summary>
    /// The concrete provider / infrastructure adapter assemblies. Nothing in an inward layer may
    /// reference any of these (Req 3.8).
    /// </summary>
    private static readonly string[] ForbiddenProviderAssemblies =
    {
        "Messaging.MassTransit",
        "Messaging.Kafka",
        "Messaging.Batch",
        "Messaging.Persistence",
    };

    [Fact]
    public void DomainAssembly_ReferencesNoConcreteProvider()
    {
        // Arrange: anchor on a type that genuinely lives in the Domain assembly.
        var domainAssembly = typeof(IMessagePublisher).Assembly;

        // Act
        var offendingReferences = ForbiddenReferencesOf(domainAssembly);

        // Assert
        Assert.Empty(offendingReferences);
    }

    [Fact]
    public void ApplicationAssembly_ReferencesNoConcreteProvider()
    {
        // Arrange: anchor on a type that genuinely lives in the Application assembly.
        var applicationAssembly = typeof(PublishPipeline).Assembly;

        // Act
        var offendingReferences = ForbiddenReferencesOf(applicationAssembly);

        // Assert
        Assert.Empty(offendingReferences);
    }

    private static string[] ForbiddenReferencesOf(Assembly assembly) =>
        assembly
            .GetReferencedAssemblies()
            .Select(reference => reference.Name)
            .Where(name => name is not null && ForbiddenProviderAssemblies.Contains(name))
            .Select(name => name!)
            .ToArray();
}
