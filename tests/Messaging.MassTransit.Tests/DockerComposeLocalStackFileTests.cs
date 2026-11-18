using System.Text.RegularExpressions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Plain file-reading checks on the repo's docker-compose.yml (no containers).
/// Validates: Requirements 19.1, 19.2, 19.3, 19.4, 19.6.
/// </summary>
public sealed class DockerComposeLocalStackFileTests
{
    // Snapshot of the kafka service block, taken before the localstack service was added.
    private const string ExpectedKafkaBlock = """
          kafka:
            # Pinned release tag, identical to KafkaContainerFixture. Never use "latest".
            image: confluentinc/cp-kafka:7.6.1
            container_name: messaging-core-kafka
            ports:
              - "9092:9092"
            environment:
              # Single-node KRaft (no ZooKeeper): this node is both broker and controller.
              KAFKA_NODE_ID: 1
              KAFKA_PROCESS_ROLES: broker,controller
              KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
              KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
              # Public (non-secret) KRaft cluster identifier for this disposable local broker.
              CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
              # PLAINTEXT_HOST is reachable from the host; PLAINTEXT is for in-container/internal use.
              KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
              KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
              KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
              KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
              # Single-node replication settings.
              KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
              KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
              KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
            healthcheck:
              test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafka:29092 > /dev/null 2>&1"]
              interval: 5s
              timeout: 10s
              retries: 6
              start_period: 20s
            restart: "no"
        """;

    [Fact]
    public void Compose_LocalStackImage_DefaultEqualsPinnedTag()
    {
        var localstack = ServiceBlock("localstack");

        var match = Regex.Match(localstack, @"image:\s*localstack/localstack:\$\{LOCALSTACK_IMAGE_TAG:-([^}]+)\}");

        Assert.True(match.Success, "localstack image must use ${LOCALSTACK_IMAGE_TAG:-<default>}.");
        Assert.Equal(LocalStackImageTag.DefaultTag, match.Groups[1].Value);
    }

    [Fact]
    public void Compose_LocalStackHealthcheck_ProbesSqsAndSns()
    {
        var localstack = ServiceBlock("localstack");

        Assert.Contains("awslocal sqs list-queues", localstack);
        Assert.Contains("awslocal sns list-topics", localstack);
    }

    [Fact]
    public void Compose_LocalStackService_HasNoAuthTokenAccessKeyOrLatest()
    {
        var localstack = ServiceBlock("localstack");
        var code = string.Join('\n', localstack.Split('\n').Where(l => !l.TrimStart().StartsWith('#')));

        Assert.DoesNotContain("LOCALSTACK_AUTH_TOKEN", code);
        Assert.DoesNotContain("ACCESS_KEY", code, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SECRET", code, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("latest", code, StringComparison.OrdinalIgnoreCase);
        Assert.False(Regex.IsMatch(code, @"AKIA[0-9A-Z]{12,}"), "No AWS access key id may appear.");
    }

    [Fact]
    public void Compose_KafkaService_IsUnchanged()
    {
        var expected = Normalize(ExpectedKafkaBlock);
        var actual = Normalize(ServiceBlock("kafka"));

        Assert.Equal(expected, actual);
    }

    private static string Normalize(string text) =>
        string.Join('\n', text.Replace("\r\n", "\n").Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0));

    /// <summary>Returns the lines of one top-level service, from its header to the next sibling.</summary>
    private static string ServiceBlock(string name)
    {
        var lines = File.ReadAllText(FindCompose()).Replace("\r\n", "\n").Split('\n');
        var header = $"  {name}:";
        var start = Array.FindIndex(lines, l => l.TrimEnd() == header);
        Assert.True(start >= 0, $"Service '{name}' not found in docker-compose.yml.");

        var block = new List<string> { lines[start] };
        for (var i = start + 1; i < lines.Length; i++)
        {
            var line = lines[i];
            var isSiblingKey = Regex.IsMatch(line, @"^  [A-Za-z0-9_-]+:\s*$");
            var isSiblingComment = line.StartsWith("  #") && !line.StartsWith("   ");
            if (isSiblingKey || isSiblingComment || (line.Length > 0 && !line.StartsWith(' ')))
            {
                break;
            }

            block.Add(line);
        }

        return string.Join('\n', block);
    }

    private static string FindCompose()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "docker-compose.yml");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException("docker-compose.yml not found above " + AppContext.BaseDirectory);
    }
}
