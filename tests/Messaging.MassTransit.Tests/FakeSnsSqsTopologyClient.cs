using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

/// <summary>The <see cref="ISnsSqsTopologyClient"/> operations a test can inject a failure into.</summary>
internal enum SnsSqsTopologyOperation
{
    GetQueueUrl,
    GetQueueAttributes,
    CreateQueue,
    SetQueueAttributes,
    GetTopicArn,
    CreateTopic,
    ListSubscriptionsByTopic,
    Subscribe,
    Ping,
}

/// <summary>
/// In-memory <see cref="ISnsSqsTopologyClient"/> for topology tests. Holds queues, topics, and
/// subscriptions; counts mutating calls (create, set attributes, subscribe) so idempotency tests can
/// assert "no mutation"; and lets a test preset state and inject a failure per operation, optionally
/// scoped to one resource (queue name or URL, topic name or ARN).
/// </summary>
internal sealed class FakeSnsSqsTopologyClient : ISnsSqsTopologyClient
{
    private const string Account = "000000000000";
    private const string Region = "us-east-1";

    private readonly object _gate = new();
    private readonly Dictionary<string, Dictionary<string, string>> _queuesByUrl = new();
    private readonly Dictionary<string, string> _queueUrlsByName = new();
    private readonly Dictionary<string, Dictionary<string, string>> _topicsByArn = new();
    private readonly Dictionary<string, string> _topicArnsByName = new();
    private readonly Dictionary<string, List<SnsSubscriptionInfo>> _subscriptionsByTopicArn = new();
    private readonly Dictionary<(SnsSqsTopologyOperation Operation, string? Resource), Exception> _failures = new();
    private int _mutationCount;

    /// <summary>The number of create, set-attributes, and subscribe calls that succeeded.</summary>
    public int MutationCount => Volatile.Read(ref _mutationCount);

    /// <summary>Names of operations invoked, in call order (including failed ones).</summary>
    public List<string> Calls { get; } = new();

    public static string QueueUrlFor(string queueName) => $"https://sqs.{Region}.localhost/{Account}/{queueName}";

    public static string QueueArnFor(string queueName) => $"arn:aws:sqs:{Region}:{Account}:{queueName}";

    public static string TopicArnFor(string topicName) => $"arn:aws:sns:{Region}:{Account}:{topicName}";

    /// <summary>Presets an existing queue (not counted as a mutation) and returns its URL.</summary>
    public string PresetQueue(string queueName, IReadOnlyDictionary<string, string>? attributes = null)
    {
        lock (_gate)
        {
            var url = QueueUrlFor(queueName);
            _queueUrlsByName[queueName] = url;
            var stored = attributes is null ? new Dictionary<string, string>() : new Dictionary<string, string>(attributes);
            stored.TryAdd("QueueArn", QueueArnFor(queueName));
            _queuesByUrl[url] = stored;
            return url;
        }
    }

    /// <summary>Presets an existing topic (not counted as a mutation) and returns its ARN.</summary>
    public string PresetTopic(string topicName, IReadOnlyDictionary<string, string>? attributes = null)
    {
        lock (_gate)
        {
            var arn = TopicArnFor(topicName);
            _topicArnsByName[topicName] = arn;
            _topicsByArn[arn] = attributes is null ? new Dictionary<string, string>() : new Dictionary<string, string>(attributes);
            _subscriptionsByTopicArn.TryAdd(arn, new List<SnsSubscriptionInfo>());
            return arn;
        }
    }

    /// <summary>Presets a subscription on an existing topic (not counted as a mutation).</summary>
    public void PresetSubscription(string topicArn, string queueArn, string protocol = "sqs")
    {
        lock (_gate)
        {
            if (!_subscriptionsByTopicArn.TryGetValue(topicArn, out var list))
            {
                list = new List<SnsSubscriptionInfo>();
                _subscriptionsByTopicArn[topicArn] = list;
            }

            list.Add(new SnsSubscriptionInfo($"{topicArn}:{Guid.NewGuid():N}", protocol, queueArn));
        }
    }

    /// <summary>Presets or replaces the attributes of an existing queue (not counted as a mutation).</summary>
    public void PresetQueueAttributes(string queueUrl, IReadOnlyDictionary<string, string> attributes)
    {
        lock (_gate)
        {
            _queuesByUrl[queueUrl] = new Dictionary<string, string>(attributes);
        }
    }

    /// <summary>
    /// Makes <paramref name="operation"/> throw <paramref name="exception"/>. When <paramref name="resource"/>
    /// is given, only calls whose name, URL, or ARN argument equals it fail; otherwise every call fails.
    /// </summary>
    public void FailOn(SnsSqsTopologyOperation operation, Exception exception, string? resource = null)
    {
        lock (_gate)
        {
            _failures[(operation, resource)] = exception;
        }
    }

    /// <summary>Returns the current attributes of a queue by URL, or null when absent.</summary>
    public IReadOnlyDictionary<string, string>? SnapshotQueueAttributes(string queueUrl)
    {
        lock (_gate)
        {
            return _queuesByUrl.TryGetValue(queueUrl, out var a) ? new Dictionary<string, string>(a) : null;
        }
    }

    public Task<string?> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.GetQueueUrl, queueName, cancellationToken);
            return Task.FromResult(_queueUrlsByName.TryGetValue(queueName, out var url) ? url : null);
        }
    }

    public Task<IReadOnlyDictionary<string, string>?> GetQueueAttributesAsync(
        string queueUrl, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.GetQueueAttributes, queueUrl, cancellationToken);
            IReadOnlyDictionary<string, string>? result =
                _queuesByUrl.TryGetValue(queueUrl, out var a) ? new Dictionary<string, string>(a) : null;
            return Task.FromResult(result);
        }
    }

    public Task<string> CreateQueueAsync(
        string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.CreateQueue, queueName, cancellationToken);
            _mutationCount++;
            if (_queueUrlsByName.TryGetValue(queueName, out var existing))
            {
                return Task.FromResult(existing);
            }

            var url = PresetQueue(queueName, attributes);
            return Task.FromResult(url);
        }
    }

    public Task SetQueueAttributesAsync(
        string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.SetQueueAttributes, queueUrl, cancellationToken);
            if (!_queuesByUrl.TryGetValue(queueUrl, out var stored))
            {
                throw new InvalidOperationException($"Queue '{queueUrl}' does not exist.");
            }

            _mutationCount++;
            foreach (var pair in attributes)
            {
                stored[pair.Key] = pair.Value;
            }

            return Task.CompletedTask;
        }
    }

    public Task<string?> GetTopicArnAsync(string topicName, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.GetTopicArn, topicName, cancellationToken);
            return Task.FromResult(_topicArnsByName.TryGetValue(topicName, out var arn) ? arn : null);
        }
    }

    public Task<string> CreateTopicAsync(
        string topicName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.CreateTopic, topicName, cancellationToken);
            _mutationCount++;
            if (_topicArnsByName.TryGetValue(topicName, out var existing))
            {
                return Task.FromResult(existing);
            }

            return Task.FromResult(PresetTopic(topicName, attributes));
        }
    }

    public Task<IReadOnlyList<SnsSubscriptionInfo>> ListSubscriptionsByTopicAsync(
        string topicArn, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.ListSubscriptionsByTopic, topicArn, cancellationToken);
            IReadOnlyList<SnsSubscriptionInfo> result = _subscriptionsByTopicArn.TryGetValue(topicArn, out var list)
                ? list.ToList()
                : Array.Empty<SnsSubscriptionInfo>();
            return Task.FromResult(result);
        }
    }

    public Task<string> SubscribeAsync(
        string topicArn,
        string queueArn,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.Subscribe, topicArn, cancellationToken);
            if (!_topicsByArn.ContainsKey(topicArn))
            {
                throw new InvalidOperationException($"Topic '{topicArn}' does not exist.");
            }

            _mutationCount++;
            var existing = _subscriptionsByTopicArn[topicArn]
                .FirstOrDefault(s => s.Protocol == "sqs" && s.Endpoint == queueArn);
            if (existing is not null)
            {
                return Task.FromResult(existing.SubscriptionArn);
            }

            var subscriptionArn = $"{topicArn}:{Guid.NewGuid():N}";
            _subscriptionsByTopicArn[topicArn].Add(new SnsSubscriptionInfo(subscriptionArn, "sqs", queueArn));
            return Task.FromResult(subscriptionArn);
        }
    }

    /// <summary>Optional override that replaces the default ping, e.g. to simulate a slow ping.</summary>
    public Func<CancellationToken, Task>? PingOverride { get; set; }

    public Task PingAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            Enter(SnsSqsTopologyOperation.Ping, null, cancellationToken);
        }

        return PingOverride?.Invoke(cancellationToken) ?? Task.CompletedTask;
    }

    // Caller holds _gate. Records the call, honors cancellation, then throws an injected failure.
    private void Enter(SnsSqsTopologyOperation operation, string? resource, CancellationToken cancellationToken)
    {
        Calls.Add(operation.ToString());
        cancellationToken.ThrowIfCancellationRequested();

        if (resource is not null && _failures.TryGetValue((operation, resource), out var scoped))
        {
            throw scoped;
        }

        if (_failures.TryGetValue((operation, null), out var any))
        {
            throw any;
        }
    }
}
