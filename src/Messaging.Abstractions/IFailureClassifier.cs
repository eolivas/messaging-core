namespace Messaging.Abstractions;

/// <summary>
/// Classifies a processing failure to decide between retrying and routing straight to the
/// dead-letter sink (Req 6.2, 6.3).
/// </summary>
public interface IFailureClassifier
{
    /// <summary>
    /// Classifies the given error as <see cref="FailureKind.Transient"/> or
    /// <see cref="FailureKind.NonTransient"/> (Req 6.2, 6.3).
    /// </summary>
    /// <param name="error">The error raised while processing a message.</param>
    FailureKind Classify(Exception error);
}
