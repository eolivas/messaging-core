namespace Messaging.Persistence.Tests;

/// <summary>
/// xUnit collection that groups the persistence tests (tasks 16.3, 16.4, 16.5) so they share one
/// <see cref="PostgresContainerFixture"/> and run in a single, non-parallel collection. The fixture
/// starts exactly one PostgreSQL Testcontainer for the whole collection; the container itself is
/// held at process scope by the fixture, so <c>[Fact]</c> tests (which xUnit injects the fixture
/// into) and <c>[Property]</c> tests (which reach the same shared container through the fixture)
/// all talk to the same database.
/// </summary>
[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>
{
    /// <summary>The collection name test classes reference via <c>[Collection]</c>.</summary>
    public const string Name = "PostgresCollection";

    // Intentionally empty: this type only carries the [CollectionDefinition]/ICollectionFixture
    // wiring (xUnit convention); it is never instantiated.
}
