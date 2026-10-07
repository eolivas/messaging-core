using Messaging.Abstractions;

namespace Messaging.Tests.Fakes;

/// <summary>
/// Controls the <see cref="PublishResult"/> the <see cref="FakeMessagingProvider"/>'s publisher
/// returns, so a test can exercise both the success and the transport-failure paths of the publish
/// pipeline without a real transport. The outcome also decides whether a captured envelope is looped
/// into the consume side: only a confirmed publish is "delivered".
/// </summary>
public enum FakePublishOutcome
{
    /// <summary>
    /// The publish is confirmed: the publisher returns <see cref="PublishResult.Ok"/> and, when
    /// loopback delivery is enabled, the envelope is handed to the consumer host.
    /// </summary>
    Confirm,

    /// <summary>
    /// The publish fails: the publisher returns a failed <see cref="PublishResult"/> and the envelope
    /// is never looped into the consume side (it is left undelivered).
    /// </summary>
    Fail,
}
