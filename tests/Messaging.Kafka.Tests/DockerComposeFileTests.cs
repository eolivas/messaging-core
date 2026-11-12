using System.Text.RegularExpressions;
using Xunit;

namespace Messaging.Kafka.Tests;

/// <summary>
/// File-content assertions for the repo-root docker-compose.yml (Req 14.2, 14.3, 14.4).
/// Declarative IaC, so verified by text parsing rather than property tests.
/// </summary>
public sealed class DockerComposeFileTests
{
    private static readonly string RepoRoot = FindRepoRoot();

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "docker-compose.yml"))
                || dir.GetFiles("*.sln").Length > 0)
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Repository root (docker-compose.yml or .sln) not found.");
    }

    /// <summary>Compose lines with full-line and trailing comments removed.</summary>
    private static List<string> ReadActiveLines()
    {
        var path = Path.Combine(RepoRoot, "docker-compose.yml");
        Assert.True(File.Exists(path), $"Missing {path}");
        return File.ReadAllLines(path)
            .Select(l => Regex.Replace(l, @"(^|\s)#.*$", string.Empty).TrimEnd())
            .Where(l => l.Length > 0)
            .ToList();
    }

    private static string ReadFixtureImage()
    {
        var source = File.ReadAllText(
            Path.Combine(RepoRoot, "tests", "Messaging.Kafka.Tests", "KafkaContainerFixture.cs"));
        var match = Regex.Match(source, "\"(confluentinc/cp-kafka:[^\"]+)\"");
        Assert.True(match.Success, "Fixture image literal not found.");
        return match.Groups[1].Value;
    }

    [Fact]
    public void ComposeImage_Parsed_EqualsFixtureImage()
    {
        var lines = ReadActiveLines();

        var images = lines
            .Select(l => Regex.Match(l, @"^\s*image:\s*(\S+)\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value.Trim('"', '\''))
            .ToList();

        Assert.Single(images);
        Assert.Equal("confluentinc/cp-kafka:7.6.1", images[0]);
        Assert.Equal(ReadFixtureImage(), images[0]);
    }

    [Fact]
    public void ComposeFile_Parsed_NeverUsesLatestTag()
    {
        var text = string.Join("\n", ReadActiveLines());

        Assert.DoesNotContain(":latest", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComposeFile_Parsed_DeclaresSingleKafkaServiceWithHealthcheck()
    {
        var lines = ReadActiveLines();

        var services = lines.Where(l => Regex.IsMatch(l, @"^  [A-Za-z0-9_-]+:\s*$")).ToList();

        Assert.Single(services);
        Assert.Equal("  kafka:", services[0]);
        Assert.Contains(lines, l => Regex.IsMatch(l, @"^    healthcheck:\s*$"));
    }

    [Fact]
    public void ComposeFile_Parsed_ExposesHostReachablePlaintextListenerOn9092()
    {
        var lines = ReadActiveLines();

        Assert.Contains(lines, l => Regex.IsMatch(l, @"^\s*-\s*""?9092:9092""?\s*$"));
        Assert.Contains(lines, l =>
            l.TrimStart().StartsWith("KAFKA_ADVERTISED_LISTENERS:", StringComparison.Ordinal)
            && l.Contains("PLAINTEXT_HOST://localhost:9092", StringComparison.Ordinal));
        Assert.Contains(lines, l =>
            l.TrimStart().StartsWith("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:", StringComparison.Ordinal)
            && l.Contains("PLAINTEXT_HOST:PLAINTEXT", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("SASL")]
    [InlineData("SSL")]
    [InlineData("TLS")]
    [InlineData("PASSWORD")]
    [InlineData("KEYSTORE")]
    [InlineData("TRUSTSTORE")]
    [InlineData("PRIVATE_KEY")]
    [InlineData("PRIVATE-KEY")]
    [InlineData("SECRET")]
    public void ComposeFile_Parsed_ContainsNoSecretOrSecurityKeys(string forbidden)
    {
        var text = string.Join("\n", ReadActiveLines());

        Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
    }
}
