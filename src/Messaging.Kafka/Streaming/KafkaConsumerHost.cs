using Messaging.Abstractions;
using Messaging.Consuming;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The Streaming (Kafka) consumer host shell. It owns the consumer-group iteration:
/// <b>consume → process → commit</b>, where the offset is committed <b>manually</b> and only
/// <b>after</b> the core pipeline has processed the record. Committing after (never before) the
/// pipeline is what preserves at-least-once delivery (Req 6.7): a crash between process and commit
/// reprocesses the record on restart rather than skipping it.
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle (Req 3.3) is delegated to the Application-layer <see cref="ConsumerHostStateMachine"/>:
/// the host drives Stopped → Starting → Running → Stopping → Stopped and owns no second state machine
/// (SRP). <see cref="StartAsync"/> subscribes the consumer and advertises Running;
/// <see cref="StopAsync"/> stops accepting new records (Req 10.5) and closes the consumer so the
/// group rebalances. Like the sibling adapter shells, the host does not run its own polling thread:
/// a hosted-service wrapper (task 21) repeatedly invokes <see cref="ConsumeOnceAsync"/> while the
/// host is <see cref="ConsumerHostState.Running"/>. Keeping the loop out of the shell makes the
/// consume → process → commit shape deterministic and directly testable.
/// </para>
/// <para>
/// This is an adapter shell: the deep Kafka mechanics (partition-assignment strategy, rebalance
/// callbacks, offset-reset/retention/replay) live behind the shallow <see cref="IKafkaConsumer"/>
/// seam and are owned by a later spec. The shell expresses the manual-commit-after-pipeline shape
/// through <see cref="ConsumeOnceAsync"/>, one deterministic consume → process → commit iteration.
/// </para>
/// </remarks>
public sealed class KafkaConsumerHost : IMessageConsumerHost
{
    private readonly IKafkaConsumer _consumer;
    private readonly IStreamMessageProcessor _processor;
    private readonly KafkaProviderSettings _settings;
    private readonly ILogger<KafkaConsumerHost> _logger;
    private readonly ConsumerHostStateMachine _stateMachine = new();

    /// <summary>
    /// Creates the Kafka consumer host (constructor injection only).
    /// </summary>
    /// <param name="consumer">The consumer-group seam the host polls and commits through.</param>
    /// <param name="processor">The per-record processor (the core consume pipeline seam).</param>
    /// <param name="settings">The Kafka provider settings (topic, consumer group).</param>
    /// <param name="logger">Structured logger for the consume/commit lifecycle.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public KafkaConsumerHost(
        IKafkaConsumer consumer,
        IStreamMessageProcessor processor,
        KafkaProviderSettings settings,
        ILogger<KafkaConsumerHost> logger)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _settings.Validate();
    }

    /// <inheritdoc />
    public ConsumerHostState State => _stateMachine.State;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _stateMachine.BeginStart();

        // Join the consumer group and take a partition assignment. The hosted-service wrapper that
        // drives ConsumeOnceAsync is wired by a later spec; the shell advertises it is running and
        // ready to accept consume iterations.
        _consumer.Subscribe(_settings.Topic);

        _stateMachine.CompleteStart();

        _logger.LogInformation(
            "Kafka consumer host started: subscribed to topic {Topic} in consumer group {ConsumerGroupId}.",
            _settings.Topic,
            _settings.ConsumerGroupId);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _stateMachine.BeginStop();

        // Stop accepting new records (Req 10.5) and leave the consumer group so the broker rebalances
        // the partitions onto the remaining members. Any in-flight iteration already past its poll
        // completes and commits before the driver stops calling ConsumeOnceAsync.
        _consumer.Close();

        _stateMachine.CompleteStop();
        _logger.LogInformation(
            "Kafka consumer host stopped and left consumer group {ConsumerGroupId}.",
            _settings.ConsumerGroupId);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Runs a single consume → process → commit iteration: polls one record, runs it through the
    /// pipeline, then commits its offset — in that order, so the commit happens strictly after the
    /// pipeline (Req 6.7). An empty poll is a no-op. Rejected unless the host is
    /// <see cref="ConsumerHostState.Running"/> (Req 10.5). The hosted-service wrapper (task 21) calls
    /// this in a loop while Running; a test drives it directly to assert the ordering.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the poll/process/commit.</param>
    /// <returns><c>true</c> if a record was processed and committed; <c>false</c> on an empty poll.</returns>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not Running.</exception>
    public async Task<bool> ConsumeOnceAsync(CancellationToken cancellationToken)
    {
        if (_stateMachine.State != ConsumerHostState.Running)
        {
            // Fail fast: records are consumed only while Running. Before Start or during/after Stop
            // the host does not accept new consume work (Req 10.5).
            throw new InvalidConsumerHostStateException(_stateMachine.State, "consume a record on");
        }

        KafkaConsumeResult? result = await _consumer.ConsumeAsync(cancellationToken).ConfigureAwait(false);

        if (result is null)
        {
            // Empty poll: no record available this cycle. Nothing to process or commit.
            return false;
        }

        // Process through the core pipeline FIRST — bind contract, idempotency, handler,
        // retry/dead-letter all run behind the processor seam.
        await _processor.ProcessAsync(result.Envelope, cancellationToken).ConfigureAwait(false);

        // Commit the offset ONLY after the pipeline completed. This ordering is the at-least-once
        // guarantee (Req 6.7): a crash before this line reprocesses the record on restart.
        await _consumer.CommitAsync(result, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Kafka consumer processed and committed offset {Offset} on partition {Partition} for correlation id {CorrelationId}.",
            result.Offset,
            result.Partition,
            result.Envelope.CorrelationId);

        return true;
    }
}
