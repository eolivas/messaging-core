namespace Demo.SalesLeads.Application;

/// <summary>Random mock data generator that always stays inside the contract bounds.</summary>
public sealed class MockDataGenerator : IMockDataGenerator
{
    private static readonly string[] FirstNames =
        ["Ada", "Grace", "Linus", "Margaret", "Alan", "Edsger", "Barbara", "Dennis", "Radia", "Ken"];

    private static readonly string[] LastNames =
        ["Lovelace", "Hopper", "Torvalds", "Hamilton", "Turing", "Dijkstra", "Liskov", "Ritchie", "Perlman", "Thompson"];

    private static readonly string[] Domains = ["example.com", "example.org", "example.net"];

    public string GenerateName() =>
        $"{Pick(FirstNames)} {Pick(LastNames)}";

    public string GenerateEmail() =>
        $"{Pick(FirstNames).ToLowerInvariant()}.{Pick(LastNames).ToLowerInvariant()}" +
        $"{Random.Shared.Next(1, 10_000)}@{Pick(Domains)}";

    public decimal GenerateAmount() =>
        Random.Shared.NextInt64(1, 99_999_999L + 1) / 100m;

    private static string Pick(string[] values) => values[Random.Shared.Next(values.Length)];
}
