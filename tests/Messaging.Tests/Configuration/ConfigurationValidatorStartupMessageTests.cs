using System;
using System.Linq;
using Messaging.Abstractions;
using Messaging.Configuration;
using Xunit;

namespace Messaging.Tests.Configuration;

// Startup failure-message unit tests for Task 5.4: the mode must be present (Req 1.2) and, when a
// value is supplied, it must be a recognized MessagingMode with the complete recognized set
// reported otherwise (Req 1.3, 2.4). These are deterministic example-based tests; the broader
// range/set and missing-value guarantees are covered by the property tests in Tasks 5.2 and 5.3.
public class ConfigurationValidatorStartupMessageTests
{
    // A configuration whose only concern is the Mode under test: every other section keeps its
    // documented defaults, which sit inside their permitted ranges, so Mode is the sole variable.
    private static MessagingConfiguration ConfigurationWithMode(MessagingMode? mode) => new()
    {
        Mode = mode,
    };

    private static ConfigurationError SingleModeError(ConfigurationValidationResult result)
    {
        var modeErrors = result.Errors
            .Where(error => error.SettingName == nameof(MessagingConfiguration.Mode))
            .ToList();

        return Assert.Single(modeErrors);
    }

    [Fact]
    public void Validate_MissingMode_ReturnsInvalidWithMissingModeError()
    {
        // Arrange
        var validator = new ConfigurationValidator();
        var configuration = ConfigurationWithMode(null);

        // Act
        var result = validator.Validate(configuration);

        // Assert
        Assert.False(result.IsValid);
        var error = SingleModeError(result);
        Assert.Contains("missing", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_UnrecognizedMode_ReturnsInvalidWithModeError()
    {
        // Arrange
        var validator = new ConfigurationValidator();
        var undefinedMode = (MessagingMode)999;
        var configuration = ConfigurationWithMode(undefinedMode);

        // Act
        var result = validator.Validate(configuration);

        // Assert
        Assert.False(result.IsValid);
        var error = SingleModeError(result);
        Assert.False(string.IsNullOrWhiteSpace(error.PermittedValuesOrRange));
    }

    [Theory]
    [InlineData(MessagingMode.PublishSubscribe)]
    [InlineData(MessagingMode.MessageQueue)]
    [InlineData(MessagingMode.Streaming)]
    [InlineData(MessagingMode.BatchProcessing)]
    public void Validate_UnrecognizedMode_ReportsCompleteRecognizedSet(MessagingMode recognizedMode)
    {
        // Arrange
        var validator = new ConfigurationValidator();
        var undefinedMode = (MessagingMode)999;
        var configuration = ConfigurationWithMode(undefinedMode);

        // Act
        var result = validator.Validate(configuration);

        // Assert: every recognized mode name appears in the reported permitted set (Req 1.3, 2.4).
        var error = SingleModeError(result);
        Assert.Contains(recognizedMode.ToString(), error.PermittedValuesOrRange);
    }
}
