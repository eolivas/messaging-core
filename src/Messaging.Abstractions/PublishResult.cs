namespace Messaging.Abstractions;

/// <summary>
/// The outcome of a publish attempt (Req 3.1, 3.2). A failed result means the envelope was
/// left undelivered and <see cref="Error"/> describes the validation or transport failure.
/// </summary>
/// <param name="Succeeded">Whether the publish succeeded.</param>
/// <param name="Error">Failure description when <paramref name="Succeeded"/> is false; otherwise null.</param>
public sealed record PublishResult(bool Succeeded, string? Error)
{
    /// <summary>Creates a successful result.</summary>
    public static PublishResult Ok() => new(true, null);

    /// <summary>Creates a failed result carrying the failure reason (Req 3.2).</summary>
    public static PublishResult Fail(string error) => new(false, error);
}
