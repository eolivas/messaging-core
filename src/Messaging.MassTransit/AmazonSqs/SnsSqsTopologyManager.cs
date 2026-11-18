using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Creates or verifies the SNS topics, SQS queues, dead-letter queues, subscriptions, and queue policies
/// of a <see cref="TopologyPlan"/> over <see cref="ISnsSqsTopologyClient"/>. Existing resources are never
/// modified, except that the queue policy gains the statement for a subscribed topic.
/// </summary>
public sealed class SnsSqsTopologyManager
{
    /// <summary>Maximum SQS message retention (14 days), applied to dead-letter queues.</summary>
    public const int DeadLetterRetentionSeconds = 1_209_600;

    private const string KindQueue = "Queue";
    private const string KindTopic = "Topic";
    private const string KindSubscription = "Subscription";
    private const string KindPolicy = "QueuePolicy";

    private readonly ISnsSqsTopologyClient _client;
    private readonly AmazonSqsProviderSettings _settings;
    private readonly ILogger<SnsSqsTopologyManager> _logger;
    private readonly ConcurrentDictionary<string, string> _topicArnCache = new(StringComparer.Ordinal);

    /// <summary>Creates the manager.</summary>
    /// <param name="client">The SNS/SQS control-plane port.</param>
    /// <param name="settings">The provider settings.</param>
    /// <param name="logger">The logger.</param>
    public SnsSqsTopologyManager(
        ISnsSqsTopologyClient client,
        AmazonSqsProviderSettings settings,
        ILogger<SnsSqsTopologyManager> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Creates (or, when <paramref name="autoCreate"/> is false, verifies) the planned topology.</summary>
    /// <param name="plan">The desired topology.</param>
    /// <param name="autoCreate">True to create missing resources; false to verify only, with no mutating call.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="TopologyResourceMissingException">Verify-only and at least one resource is missing.</exception>
    /// <exception cref="TopologyConflictException">An existing queue's FIFO type disagrees with the plan.</exception>
    /// <exception cref="TopologyOperationFailedException">An SDK call failed.</exception>
    public async Task ApplyAsync(TopologyPlan plan, bool autoCreate, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var missing = new List<string>();
        var queues = new Dictionary<string, QueueState>(StringComparer.Ordinal);
        var topics = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var spec in plan.Queues.OrderBy(q => q.IsDeadLetter ? 0 : 1))
        {
            var state = await EnsureQueueAsync(spec, queues, autoCreate, missing, cancellationToken)
                .ConfigureAwait(false);
            if (state is not null)
            {
                queues[spec.Name] = state;
            }
        }

        foreach (var spec in plan.Topics)
        {
            var arn = await EnsureTopicCoreAsync(spec.Name, spec.Fifo, spec.KmsKeyId, autoCreate, missing,
                cancellationToken).ConfigureAwait(false);
            if (arn is not null)
            {
                topics[spec.Name] = arn;
            }
        }

        foreach (var sub in plan.Subscriptions)
        {
            await EnsureSubscriptionAsync(sub, queues, topics, autoCreate, missing, cancellationToken)
                .ConfigureAwait(false);
        }

        if (missing.Count > 0)
        {
            throw new TopologyResourceMissingException(missing);
        }
    }

    /// <summary>Ensures a topic exists for the publish path and returns its ARN. Cached per process.</summary>
    /// <param name="topicName">The topic name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The topic ARN.</returns>
    /// <exception cref="TopologyResourceMissingException">Verify-only and the topic does not exist.</exception>
    public async Task<string> EnsureTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        if (_topicArnCache.TryGetValue(topicName, out var cached))
        {
            return cached;
        }

        var missing = new List<string>();
        var kms = string.IsNullOrWhiteSpace(_settings.KmsMasterKeyId) ? null : _settings.KmsMasterKeyId;
        var arn = await EnsureTopicCoreAsync(
            topicName, _settings.Fifo, kms, _settings.AutoCreateTopology, missing, cancellationToken)
            .ConfigureAwait(false);
        if (arn is null)
        {
            throw new TopologyResourceMissingException(missing);
        }

        _topicArnCache[topicName] = arn;
        return arn;
    }

    private async Task<QueueState?> EnsureQueueAsync(
        QueueSpec spec,
        Dictionary<string, QueueState> known,
        bool autoCreate,
        List<string> missing,
        CancellationToken ct)
    {
        var url = await CallAsync("sqs:GetQueueUrl", spec.Name, () => _client.GetQueueUrlAsync(spec.Name, ct), ct)
            .ConfigureAwait(false);
        if (url is not null)
        {
            var attributes = await CallAsync(
                "sqs:GetQueueAttributes", spec.Name, () => _client.GetQueueAttributesAsync(url, ct), ct)
                .ConfigureAwait(false);
            if (attributes is not null)
            {
                var existingFifo = attributes.TryGetValue("FifoQueue", out var f)
                    && string.Equals(f, "true", StringComparison.OrdinalIgnoreCase);
                if (existingFifo != spec.Fifo)
                {
                    throw new TopologyConflictException(spec.Name, existingFifo ? "FIFO" : "Standard");
                }

                attributes.TryGetValue("QueueArn", out var existingArn);
                attributes.TryGetValue("Policy", out var policy);
                _logger.LogInformation(
                    "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindQueue, spec.Name, "Exists");
                return new QueueState(url, existingArn ?? string.Empty, policy);
            }
        }

        if (!autoCreate)
        {
            missing.Add(spec.Name);
            _logger.LogWarning(
                "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindQueue, spec.Name, "Missing");
            return null;
        }

        var attrs = await BuildQueueAttributesAsync(spec, known, ct).ConfigureAwait(false);
        var createdUrl = await CallAsync(
            "sqs:CreateQueue", spec.Name, () => _client.CreateQueueAsync(spec.Name, attrs, ct), ct)
            .ConfigureAwait(false);
        var created = await CallAsync(
            "sqs:GetQueueAttributes", spec.Name, () => _client.GetQueueAttributesAsync(createdUrl, ct), ct)
            .ConfigureAwait(false);
        string? arn = null;
        created?.TryGetValue("QueueArn", out arn);
        _logger.LogInformation(
            "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindQueue, spec.Name, "Created");
        return new QueueState(createdUrl, arn ?? string.Empty, null);
    }

    private async Task<Dictionary<string, string>> BuildQueueAttributesAsync(
        QueueSpec spec, Dictionary<string, QueueState> known, CancellationToken ct)
    {
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["VisibilityTimeout"] = spec.VisibilityTimeoutSeconds.ToString(CultureInfo.InvariantCulture),
            ["MessageRetentionPeriod"] = (spec.IsDeadLetter ? DeadLetterRetentionSeconds : spec.RetentionSeconds)
                .ToString(CultureInfo.InvariantCulture),
        };
        if (spec.Fifo)
        {
            attrs["FifoQueue"] = "true";
        }

        if (!string.IsNullOrWhiteSpace(spec.KmsKeyId))
        {
            attrs["KmsMasterKeyId"] = spec.KmsKeyId;
        }

        if (!spec.IsDeadLetter && spec.DeadLetterTarget is not null && spec.MaxReceiveCount is not null)
        {
            string? dlqArn = null;
            if (known.TryGetValue(spec.DeadLetterTarget, out var dlq))
            {
                dlqArn = dlq.Arn;
            }
            else
            {
                var target = spec.DeadLetterTarget;
                var url = await CallAsync("sqs:GetQueueUrl", target, () => _client.GetQueueUrlAsync(target, ct), ct)
                    .ConfigureAwait(false);
                var a = url is null
                    ? null
                    : await CallAsync(
                        "sqs:GetQueueAttributes", target, () => _client.GetQueueAttributesAsync(url, ct), ct)
                        .ConfigureAwait(false);
                a?.TryGetValue("QueueArn", out dlqArn);
            }

            if (string.IsNullOrEmpty(dlqArn))
            {
                throw new TopologyResourceMissingException(new[] { spec.DeadLetterTarget });
            }

            attrs["RedrivePolicy"] = JsonSerializer.Serialize(new Dictionary<string, object>
            {
                ["deadLetterTargetArn"] = dlqArn,
                ["maxReceiveCount"] = spec.MaxReceiveCount.Value,
            });
        }

        return attrs;
    }

    private async Task<string?> EnsureTopicCoreAsync(
        string name, bool fifo, string? kmsKeyId, bool autoCreate, List<string> missing, CancellationToken ct)
    {
        var arn = await CallAsync("sns:GetTopicAttributes", name, () => _client.GetTopicArnAsync(name, ct), ct)
            .ConfigureAwait(false);
        if (arn is not null)
        {
            _logger.LogInformation(
                "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindTopic, name, "Exists");
            return arn;
        }

        if (!autoCreate)
        {
            missing.Add(name);
            _logger.LogWarning(
                "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindTopic, name, "Missing");
            return null;
        }

        var attrs = new Dictionary<string, string>(StringComparer.Ordinal);
        if (fifo)
        {
            attrs["FifoTopic"] = "true";
        }

        if (!string.IsNullOrWhiteSpace(kmsKeyId))
        {
            attrs["KmsMasterKeyId"] = kmsKeyId;
        }

        var created = await CallAsync("sns:CreateTopic", name, () => _client.CreateTopicAsync(name, attrs, ct), ct)
            .ConfigureAwait(false);
        _logger.LogInformation(
            "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindTopic, name, "Created");
        return created;
    }

    private async Task EnsureSubscriptionAsync(
        SubscriptionSpec spec,
        Dictionary<string, QueueState> queues,
        Dictionary<string, string> topics,
        bool autoCreate,
        List<string> missing,
        CancellationToken ct)
    {
        var resource = $"{spec.TopicName}->{spec.QueueName}";
        if (!queues.TryGetValue(spec.QueueName, out var queue) || !topics.TryGetValue(spec.TopicName, out var topicArn))
        {
            // A subscription cannot exist without both ends.
            missing.Add(resource);
            return;
        }

        var existing = await CallAsync(
            "sns:ListSubscriptionsByTopic", resource, () => _client.ListSubscriptionsByTopicAsync(topicArn, ct), ct)
            .ConfigureAwait(false);
        var present = existing.Any(s =>
            string.Equals(s.Protocol, "sqs", StringComparison.OrdinalIgnoreCase)
            && string.Equals(s.Endpoint, queue.Arn, StringComparison.Ordinal));

        if (!present)
        {
            if (!autoCreate)
            {
                missing.Add(resource);
                _logger.LogWarning(
                    "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindSubscription, resource, "Missing");
                return;
            }

            var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["RawMessageDelivery"] = spec.RawMessageDelivery ? "true" : "false",
            };
            await CallAsync("sns:Subscribe", resource, () => _client.SubscribeAsync(topicArn, queue.Arn, attrs, ct), ct)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindSubscription, resource, "Created");
        }
        else
        {
            _logger.LogInformation(
                "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindSubscription, resource, "Exists");
        }

        if (!autoCreate)
        {
            return;
        }

        var sid = QueuePolicyDocument.SidFor(topicArn);
        var hasStatement = queue.Policy is not null && queue.Policy.Contains(sid, StringComparison.Ordinal);
        if (present && hasStatement)
        {
            return;
        }

        var merged = QueuePolicyDocument.Merge(queue.Policy, queue.Arn, new[] { topicArn });
        var policyAttrs = new Dictionary<string, string>(StringComparer.Ordinal) { ["Policy"] = merged };
        await CallAsync(
            "sqs:SetQueueAttributes", spec.QueueName, () => _client.SetQueueAttributesAsync(queue.Url, policyAttrs, ct), ct)
            .ConfigureAwait(false);
        queues[spec.QueueName] = queue with { Policy = merged };
        _logger.LogInformation(
            "Topology {ResourceKind} {ResourceName} outcome {Outcome}", KindPolicy, spec.QueueName, "Updated");
    }

    private static async Task<T> CallAsync<T>(
        string operation, string resource, Func<Task<T>> call, CancellationToken ct)
    {
        try
        {
            return await call().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is not (TopologyConflictException
            or TopologyOperationFailedException or TopologyResourceMissingException))
        {
            ct.ThrowIfCancellationRequested();
            throw new TopologyOperationFailedException(operation, resource, ex);
        }
    }

    private static Task CallAsync(string operation, string resource, Func<Task> call, CancellationToken ct) =>
        CallAsync<bool>(operation, resource, async () =>
        {
            await call().ConfigureAwait(false);
            return true;
        }, ct);

    private sealed record QueueState(string Url, string Arn, string? Policy);
}
