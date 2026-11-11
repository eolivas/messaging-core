using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Resilience;
using Xunit;

namespace Messaging.Tests.Resilience;

// Unit tests for FailureClassifier (missing-idempotency-key-classification-fix).
// Validates: Requirements 1.1, 2.1.
[Trait("Feature", "missing-idempotency-key-classification-fix")]
public class FailureClassifierTests
{
    [Fact]
    public void Classify_MissingIdempotencyKeyException_ReturnsNonTransient()
    {
        // Arrange
        var classifier = new FailureClassifier();
        var error = new MissingIdempotencyKeyException("orders.created.v1");

        // Act
        var kind = classifier.Classify(error);

        // Assert
        Assert.Equal(FailureKind.NonTransient, kind);
    }

    [Fact]
    public void MissingIdempotencyKeyException_Constructed_PreservesMessageTypeAndPayloadFreeMessage()
    {
        // Arrange
        const string messageType = "orders.created.v1";

        // Act
        var error = new MissingIdempotencyKeyException(messageType);

        // Assert (Req 2.5): type preserved, message names only the type, kind is NonTransient.
        Assert.Equal(messageType, error.MessageType);
        Assert.Contains(messageType, error.Message);
        Assert.Equal(
            "Idempotency is enabled but no idempotency key could be derived for message type "
            + $"'{messageType}'; the message was rejected before any side effects.",
            error.Message);
        Assert.Equal(FailureKind.NonTransient, error.Kind);
    }

    [Fact]
    public void Classify_UnrecognizedException_ReturnsTransient()
    {
        // Arrange
        var classifier = new FailureClassifier();
        var error = new InvalidOperationException("unrecognized");

        // Act
        var kind = classifier.Classify(error);

        // Assert (Req 3.4)
        Assert.Equal(FailureKind.Transient, kind);
    }
}
