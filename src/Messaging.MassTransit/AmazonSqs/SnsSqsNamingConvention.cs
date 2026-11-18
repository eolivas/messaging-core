using System.Text;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Pure derivation of SNS topic, SQS queue, and dead-letter queue names. Topic names follow
/// MassTransit's Amazon message-name formatting; queue names follow its kebab-case endpoint formatting.
/// Names are never truncated.
/// </summary>
public static class SnsSqsNamingConvention
{
    /// <summary>Maximum SNS topic name length, including <c>.fifo</c>.</summary>
    public const int TopicNameLimit = 256;

    /// <summary>Maximum SQS queue name length, including <c>.fifo</c>.</summary>
    public const int QueueNameLimit = 80;

    private const string FifoSuffix = ".fifo";

    /// <summary>Derives the topic name for a message type string.</summary>
    /// <param name="messageType">The namespace-qualified message type.</param>
    /// <param name="settings">Prefix and FIFO options.</param>
    /// <returns>The topic name.</returns>
    /// <exception cref="ResourceNameTooLongException">The name exceeds 256 characters.</exception>
    public static string TopicName(string messageType, AmazonSqsProviderSettings settings)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentNullException.ThrowIfNull(settings);

        return Finish(
            FormatMessageName(messageType), settings.TopicNamePrefix, settings.Fifo, TopicNameLimit, messageType);
    }

    /// <summary>Derives the consumer queue name for an endpoint name.</summary>
    /// <param name="endpointName">The logical endpoint name.</param>
    /// <param name="settings">Prefix and FIFO options.</param>
    /// <returns>The queue name.</returns>
    /// <exception cref="ResourceNameTooLongException">The name exceeds 80 characters.</exception>
    public static string QueueName(string endpointName, AmazonSqsProviderSettings settings)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);
        ArgumentNullException.ThrowIfNull(settings);

        return Finish(
            KebabCase(endpointName), settings.QueueNamePrefix, settings.Fifo, QueueNameLimit, endpointName);
    }

    /// <summary>Derives the dead-letter queue name, inserting the suffix before <c>.fifo</c>.</summary>
    /// <param name="queueName">The consumer queue name.</param>
    /// <param name="suffix">The dead-letter suffix.</param>
    /// <returns>The dead-letter queue name.</returns>
    /// <exception cref="ResourceNameTooLongException">The name exceeds 80 characters.</exception>
    public static string DeadLetterQueueName(string queueName, string suffix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(suffix);

        var name = queueName.EndsWith(FifoSuffix, StringComparison.Ordinal)
            ? string.Concat(queueName.AsSpan(0, queueName.Length - FifoSuffix.Length), suffix, FifoSuffix)
            : queueName + suffix;

        return name.Length > QueueNameLimit
            ? throw new ResourceNameTooLongException(queueName, QueueNameLimit)
            : name;
    }

    private static string Finish(string formatted, string prefix, bool fifo, int limit, string subject)
    {
        if (formatted.EndsWith(FifoSuffix, StringComparison.Ordinal))
        {
            formatted = formatted[..^FifoSuffix.Length];
        }

        var name = prefix + Sanitize(formatted);
        if (fifo && !name.EndsWith(FifoSuffix, StringComparison.Ordinal))
        {
            name += FifoSuffix;
        }

        return name.Length > limit ? throw new ResourceNameTooLongException(subject, limit) : name;
    }

    // MassTransit AmazonSqsMessageNameFormatter: namespace dots become '_', namespace and type join with '-'.
    private static string FormatMessageName(string messageType)
    {
        var split = messageType.LastIndexOf('.');
        var type = (split < 0 ? messageType : messageType[(split + 1)..]).Replace('+', '_');
        return split < 0 ? type : messageType[..split].Replace('.', '_') + "-" + type;
    }

    // MassTransit KebabCaseEndpointNameFormatter: lower-case words separated by '-'.
    private static string KebabCase(string name)
    {
        var builder = new StringBuilder(name.Length + 8);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c) && i > 0 && name[i - 1] is not ('-' or '_' or '.'))
            {
                var boundary = !char.IsUpper(name[i - 1])
                    || (i + 1 < name.Length && char.IsLower(name[i + 1]));
                if (boundary)
                {
                    builder.Append('-');
                }
            }

            builder.Append(char.ToLowerInvariant(c));
        }

        return builder.ToString();
    }

    private static string Sanitize(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            var permitted = c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '-';
            builder.Append(permitted ? c : '_');
        }

        return builder.ToString();
    }
}
