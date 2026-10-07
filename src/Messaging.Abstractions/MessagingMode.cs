namespace Messaging.Abstractions;

/// <summary>
/// The category of messaging behavior an instance operates in. An embedding application is
/// configured to run in exactly one mode for the lifetime of the instance (Req 1.4, 3.4).
/// </summary>
public enum MessagingMode
{
    /// <summary>Publish-Subscribe: one message fanned out to many subscribers.</summary>
    PublishSubscribe,

    /// <summary>Message Queue: point-to-point, competing-consumer delivery.</summary>
    MessageQueue,

    /// <summary>Streaming: ordered, partitioned, replayable log consumption.</summary>
    Streaming,

    /// <summary>Batch Processing: scheduled/triggered bounded-window processing.</summary>
    BatchProcessing,
}
