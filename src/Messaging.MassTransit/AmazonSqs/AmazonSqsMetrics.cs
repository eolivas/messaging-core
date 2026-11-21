using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Provider metrics for the Amazon SNS/SQS transport: four counters (published, consumed,
/// dead-lettered, failed) tagged with the topic or queue name only, and queue-depth gauges.
/// Emission never throws, and no correlation id or payload is ever used as a tag.
/// </summary>
/// <remarks>
/// Gauges read a cache filled by <see cref="SampleQueueDepthAsync"/>. A host or background loop calls
/// that method on its own schedule, so sampling never runs on the readiness path and a sampling
/// failure cannot change readiness (Req 16.6).
/// </remarks>
public sealed class AmazonSqsMetrics : IDisposable
{
    /// <summary>The meter name for the provider metrics.</summary>
    public const string MeterName = "Messaging.MassTransit.AmazonSqs";

    /// <summary>Tag key carrying the topic name.</summary>
    public const string TopicTag = "topic";

    /// <summary>Tag key carrying the queue name.</summary>
    public const string QueueTag = "queue";

    private const string VisibleAttribute = "ApproximateNumberOfMessages";
    private const string NotVisibleAttribute = "ApproximateNumberOfMessagesNotVisible";
    private const string DelayedAttribute = "ApproximateNumberOfMessagesDelayed";

    private readonly Meter _meter;
    private readonly Counter<long> _published;
    private readonly Counter<long> _consumed;
    private readonly Counter<long> _deadLettered;
    private readonly Counter<long> _failed;
    private readonly ConcurrentDictionary<string, QueueDepth> _depths = new(StringComparer.Ordinal);
    private readonly ILogger<AmazonSqsMetrics> _logger;

    /// <summary>Creates the metrics and registers the counters and gauges.</summary>
    /// <param name="logger">Records suppressed sampling and emission failures. Defaults to a no-op logger.</param>
    public AmazonSqsMetrics(ILogger<AmazonSqsMetrics>? logger = null)
    {
        _logger = logger ?? NullLogger<AmazonSqsMetrics>.Instance;
        _meter = new Meter(MeterName);
        _published = _meter.CreateCounter<long>(
            "messaging.amazonsqs.published.count", "{message}", "Messages published to SNS or SQS.");
        _consumed = _meter.CreateCounter<long>(
            "messaging.amazonsqs.consumed.count", "{message}", "Messages consumed from SQS.");
        _deadLettered = _meter.CreateCounter<long>(
            "messaging.amazonsqs.deadletter.count", "{message}", "Messages sent to the dead-letter queue.");
        _failed = _meter.CreateCounter<long>(
            "messaging.amazonsqs.failed.count", "{message}", "Messages whose processing failed.");

        _meter.CreateObservableGauge(
            "messaging.amazonsqs.queue.depth", () => Observe(d => d.Visible), "{message}",
            "Approximate number of visible messages in the queue.");
        _meter.CreateObservableGauge(
            "messaging.amazonsqs.queue.in_flight", () => Observe(d => d.NotVisible), "{message}",
            "Approximate number of in-flight (not visible) messages in the queue.");
        _meter.CreateObservableGauge(
            "messaging.amazonsqs.queue.delayed", () => Observe(d => d.Delayed), "{message}",
            "Approximate number of delayed messages in the queue.");
    }

    /// <summary>Counts one message published to <paramref name="topicOrQueueName"/>.</summary>
    public void RecordPublished(string topicOrQueueName) => Add(_published, TopicTag, topicOrQueueName);

    /// <summary>Counts one message consumed from <paramref name="queueName"/>.</summary>
    public void RecordConsumed(string queueName) => Add(_consumed, QueueTag, queueName);

    /// <summary>Counts one message dead-lettered from <paramref name="queueName"/>.</summary>
    public void RecordDeadLettered(string queueName) => Add(_deadLettered, QueueTag, queueName);

    /// <summary>Counts one failed processing attempt on <paramref name="queueName"/>.</summary>
    public void RecordFailed(string queueName) => Add(_failed, QueueTag, queueName);

    /// <summary>
    /// Reads the depth attributes of each queue and stores them for the gauges. Failures for a queue
    /// are suppressed and leave its previous sample in place; cancellation still propagates.
    /// </summary>
    public async Task SampleQueueDepthAsync(
        ISnsSqsTopologyClient client,
        IEnumerable<string> queueNames,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(queueNames);

        foreach (var queueName in queueNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var url = await client.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
                if (url is null)
                {
                    continue;
                }

                var attributes = await client.GetQueueAttributesAsync(url, cancellationToken).ConfigureAwait(false);
                if (attributes is null)
                {
                    continue;
                }

                _depths[queueName] = new QueueDepth(
                    Read(attributes, VisibleAttribute),
                    Read(attributes, NotVisibleAttribute),
                    Read(attributes, DelayedAttribute));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // Best effort: a sampling failure must never surface (Req 16.6). It is recorded so a
                // stale gauge can be explained.
                _logger.LogWarning(
                    ex,
                    "Amazon SQS queue depth sampling failed for queue {QueueName}; keeping the previous sample.",
                    queueName);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private IEnumerable<Measurement<long>> Observe(Func<QueueDepth, long> select)
    {
        var result = new List<Measurement<long>>();
        foreach (var pair in _depths)
        {
            result.Add(new Measurement<long>(
                select(pair.Value), new KeyValuePair<string, object?>(QueueTag, pair.Key)));
        }

        return result;
    }

    private static long Read(IReadOnlyDictionary<string, string> attributes, string key) =>
        attributes.TryGetValue(key, out var text)
        && long.TryParse(text, System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture, out var value)
            ? value
            : 0;

    private void Add(Counter<long> counter, string tagKey, string name)
    {
        try
        {
            counter.Add(1, new KeyValuePair<string, object?>(tagKey, name));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Best effort: metric emission must never affect message flow (Req 15.7).
            _logger.LogWarning(ex, "Amazon SQS metric emission failed for {TagKey} {Name}.", tagKey, name);
        }
    }

    private readonly record struct QueueDepth(long Visible, long NotVisible, long Delayed);
}
