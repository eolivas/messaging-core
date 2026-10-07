namespace Messaging.Abstractions;

/// <summary>
/// The lifecycle states of an <see cref="IMessageConsumerHost"/> (Req 3.3, 10.5–10.7).
/// A host advances Stopped → Starting → Running → Stopping → Stopped.
/// </summary>
public enum ConsumerHostState
{
    /// <summary>Not consuming; the initial and terminal resting state.</summary>
    Stopped,

    /// <summary>Transitioning into the running state; not yet accepting messages.</summary>
    Starting,

    /// <summary>Actively consuming and dispatching messages.</summary>
    Running,

    /// <summary>Draining in-flight messages; no longer accepting new ones (Req 10.5).</summary>
    Stopping,
}
