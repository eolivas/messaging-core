using Messaging.Contracts;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// Records an outgoing <see cref="MessageEnvelope"/> into the transactional outbox so it commits
/// atomically with the originating state change (Req 5.1).
/// </summary>
/// <remarks>
/// <para>
/// <b>Atomicity contract.</b> An implementation records the entry into the <em>shared</em>
/// unit-of-work (the caller's <see cref="MessagingDbContext"/>) and deliberately does
/// <em>not</em> persist on its own. The caller performs its state change and the recording
/// against the same context/transaction and commits once, so the state change and the outbox row
/// commit together or neither does (Req 5.1). If the caller never commits, nothing is written.
/// </para>
/// <para>
/// This port is declared here, in the Infrastructure (persistence) package, because enlisting in
/// the shared EF unit-of-work is an Infrastructure concern and the Application/Domain layers take
/// no EF dependency (Clean Architecture, DIP). It exists to make the recorder substitutable and
/// unit-testable without pulling EF types outward.
/// </para>
/// </remarks>
public interface IOutboxRecorder
{
    /// <summary>
    /// Stages the <paramref name="envelope"/> as a pending outbox entry on the shared context
    /// <em>without</em> calling <c>SaveChanges</c>, so it enlists in the caller's transaction and
    /// commits atomically with the originating state change (Req 5.1).
    /// </summary>
    /// <param name="envelope">The outgoing envelope to record.</param>
    /// <returns>The staged <see cref="OutboxEntry"/> (tracked but not yet persisted).</returns>
    OutboxEntry Record(MessageEnvelope envelope);
}
