namespace Messaging.MassTransit.Tests;

/// <summary>
/// Runs its member classes alone, after the parallel collections finish, so timing-sensitive
/// unit tests are not starved by thread-pool or timer load from the rest of the suite.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class NonParallelCollection
{
    public const string Name = "non-parallel-timing";
}
