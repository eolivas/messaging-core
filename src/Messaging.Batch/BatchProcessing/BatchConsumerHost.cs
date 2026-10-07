using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Microsoft.Extensions.Logging;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The Batch Processing consumer host shell. Unlike a broker or a stream, batch is a
/// scheduled/triggered <b>pull → chunk → process → checkpoint</b> loop: each cycle pulls a bounded
/// window from <see cref="IBatchSource"/>, chunks it, runs each item through
/// <see cref="IBatchItemProcessor"/> (the batch stand-in for the core consume pipeline), then commits
/// a checkpoint through <see cref="IBatchCheckpointStore"/>. Checkpointing plays the role offset
/// commit / ack plays in the other modes, so a crash mid-window reprocesses from the last checkpoint
/// (at-least-once, Req 6.7).
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle (Req 3.3) is delegated to the Application-layer <see cref="ConsumerHostStateMachine"/>:
/// the host drives Stopped → Starting → Running → Stopping → Stopped and owns no second state machine
/// (SRP). While <see cref="ConsumerHostState.Running"/> a cycle can run; once
/// <see cref="StopAsync"/> moves the host to <see cref="ConsumerHostState.Stopping"/> no new cycle is
/// accepted (Req 10.5) and the in-flight cycle drains.
/// </para>
/// <para>
/// This is an adapter shell: the scheduling infrastructure (timers/triggers) and the concrete batch
/// mechanics behind the shallow ports are owned by a later spec. The shell expresses the
/// pull → chunk → process → checkpoint shape through <see cref="RunPullCycleAsync"/>, which a
/// scheduler (scheduled mode) or an external trigger (triggered mode) invokes.
/// </para>
/// </remarks>
public sealed class BatchConsumerHost : IMessageConsumerHost
{
    private readonly IBatchSource _source;
    private readonly IBatchItemProcessor _processor;
    private readonly IBatchCheckpointStore _checkpointStore;
    private readonly BatchProviderSettings _settings;
    private readonly ILogger<BatchConsumerHost> _logger;
    private readonly ConsumerHostStateMachine _stateMachine = new();

    /// <summary>
    /// Creates the batch consumer host (constructor injection only).
    /// </summary>
    /// <param name="source">The bounded-window source the host pulls from.</param>
    /// <param name="processor">The per-item processor (the core consume pipeline seam).</param>
    /// <param name="checkpointStore">The checkpoint store committed after each window.</param>
    /// <param name="settings">The batch provider settings (window size, trigger).</param>
    /// <param name="logger">Structured logger for the pull/checkpoint lifecycle.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public BatchConsumerHost(
        IBatchSource source,
        IBatchItemProcessor processor,
        IBatchCheckpointStore checkpointStore,
        BatchProviderSettings settings,
        ILogger<BatchConsumerHost> logger)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _checkpointStore = checkpointStore ?? throw new ArgumentNullException(nameof(checkpointStore));
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
        // The scheduler/trigger that invokes RunPullCycleAsync is wired by a later spec; the shell
        // simply advertises it is running and ready to accept pull cycles.
        _stateMachine.CompleteStart();
        _logger.LogInformation(
            "Batch consumer host started in {TriggerMode} mode with window size {WindowSize}.",
            _settings.TriggerMode,
            _settings.WindowSize);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _stateMachine.BeginStop();
        // No new pull cycle is accepted once Stopping; an in-flight cycle drains to its checkpoint.
        _stateMachine.CompleteStop();
        _logger.LogInformation("Batch consumer host stopped.");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Runs one scheduled/triggered pull cycle: pulls a bounded window from the source, chunks it,
    /// processes each item through the pipeline, then checkpoints the window. A checkpoint is written
    /// only after every item in the window has been processed, so the cycle is at-least-once (Req
    /// 6.7). An empty window is a no-op that writes no checkpoint. Rejected unless the host is
    /// <see cref="ConsumerHostState.Running"/> (Req 10.5).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the cycle.</param>
    /// <returns>The number of items processed and checkpointed in this cycle.</returns>
    /// <exception cref="InvalidConsumerHostStateException">If the host is not Running.</exception>
    public async Task<int> RunPullCycleAsync(CancellationToken cancellationToken)
    {
        if (_stateMachine.State != ConsumerHostState.Running)
        {
            // Fail fast: a cycle may only run while Running. Before Start or during/after Stop the
            // host does not accept new pull work (Req 10.5).
            throw new InvalidConsumerHostStateException(_stateMachine.State, "run a pull cycle on");
        }

        // Pull a bounded window (Req: scheduled/triggered pull reads a bounded window).
        IReadOnlyList<MessageEnvelope> window =
            await _source.PullWindowAsync(_settings.WindowSize, cancellationToken).ConfigureAwait(false);

        if (window.Count == 0)
        {
            _logger.LogDebug("Batch pull cycle found an empty window; nothing to process.");
            return 0;
        }

        var processed = 0;

        // Chunk the window, then process each item through the pipeline. Chunking keeps a large
        // window from being held in a single processing unit; the chunk size is the configured
        // window bound, so the shell always processes within the declared bound.
        foreach (IReadOnlyList<MessageEnvelope> chunk in Chunk(window, _settings.WindowSize))
        {
            foreach (MessageEnvelope envelope in chunk)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await _processor.ProcessAsync(envelope, cancellationToken).ConfigureAwait(false);
                processed++;
            }
        }

        // Checkpoint only after the whole window is processed — the batch equivalent of an offset
        // commit / ack (Req 6.7).
        await _checkpointStore.CheckpointAsync(processed, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Batch pull cycle processed and checkpointed {ProcessedCount} items.",
            processed);

        return processed;
    }

    private static IEnumerable<IReadOnlyList<MessageEnvelope>> Chunk(
        IReadOnlyList<MessageEnvelope> window,
        int chunkSize)
    {
        for (var start = 0; start < window.Count; start += chunkSize)
        {
            var length = Math.Min(chunkSize, window.Count - start);
            var chunk = new MessageEnvelope[length];
            for (var i = 0; i < length; i++)
            {
                chunk[i] = window[start + i];
            }

            yield return chunk;
        }
    }
}
