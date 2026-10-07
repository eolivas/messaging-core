using Messaging.Abstractions;
using Messaging.Configuration;
using Xunit;

namespace Messaging.Tests.Configuration;

// Per-mode dead-letter target unit tests for Task 13.5 (Req 8.5): the dead-letter target is
// configurable per Messaging_Mode through Messaging_Configuration (DeadLetterConfig.Target on
// MessagingConfiguration). An embedding application runs in exactly one mode for its lifetime
// (Req 1.4), so each instance supplies its own mode together with that mode's dead-letter target;
// these tests assert the configuration surface binds and exposes that per-mode target correctly.
//
// Scope note: these assert the configuration surface only. Binding the per-mode target *into the
// active provider's dead-letter sink* is deferred to the adapter/DI wiring (tasks 20/21); there is
// no sink-wiring code to exercise yet.
public class DeadLetterConfigPerModeTargetTests
{
    [Theory]
    [InlineData(MessagingMode.PublishSubscribe, "pubsub.deadletter")]
    [InlineData(MessagingMode.MessageQueue, "queue.deadletter")]
    [InlineData(MessagingMode.Streaming, "streaming.deadletter")]
    [InlineData(MessagingMode.BatchProcessing, "batch.deadletter")]
    public void DeadLetterTarget_ConfiguredForMode_ResolvesConfiguredTarget(
        MessagingMode mode,
        string target)
    {
        // Arrange & Act: a configuration pins a mode and that mode's dead-letter target.
        var configuration = new MessagingConfiguration
        {
            Mode = mode,
            DeadLetter = new DeadLetterConfig { Target = target },
        };

        // Assert: the per-mode target is bound and resolves back exactly for the configured mode.
        Assert.Equal(mode, configuration.Mode);
        Assert.Equal(target, configuration.DeadLetter.Target);
    }

    [Fact]
    public void DeadLetterTarget_NotConfigured_DefaultsToNullForProviderNativeTarget()
    {
        // Arrange & Act: no explicit dead-letter target supplied.
        var configuration = new MessagingConfiguration
        {
            Mode = MessagingMode.MessageQueue,
        };

        // Assert: a null target defers to the active provider's native dead-letter target (Req 8.5).
        Assert.Null(configuration.DeadLetter.Target);
    }

    [Fact]
    public void DeadLetterTarget_SameModeDifferentTargets_AreIndependentPerConfiguration()
    {
        // Arrange & Act: two instances of the same mode may bind distinct per-mode targets, so the
        // target travels with the configuration rather than being fixed by the mode.
        var primary = new MessagingConfiguration
        {
            Mode = MessagingMode.Streaming,
            DeadLetter = new DeadLetterConfig { Target = "streaming.deadletter.primary" },
        };
        var secondary = new MessagingConfiguration
        {
            Mode = MessagingMode.Streaming,
            DeadLetter = new DeadLetterConfig { Target = "streaming.deadletter.secondary" },
        };

        // Assert
        Assert.Equal("streaming.deadletter.primary", primary.DeadLetter.Target);
        Assert.Equal("streaming.deadletter.secondary", secondary.DeadLetter.Target);
    }
}
