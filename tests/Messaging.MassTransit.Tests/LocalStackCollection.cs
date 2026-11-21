namespace Messaging.MassTransit.Tests;

/// <summary>
/// Binds LocalStack integration tests to the shared <see cref="LocalStackContainerFixture"/>.
/// </summary>
[CollectionDefinition(Name)]
public sealed class LocalStackCollection : ICollectionFixture<LocalStackContainerFixture>
{
    public const string Name = "localstack-integration";
}
