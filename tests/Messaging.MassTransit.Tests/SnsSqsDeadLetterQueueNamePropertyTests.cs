using System;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.MassTransit.AmazonSqs;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 2: Dead-letter queue name derivation
// Validates: Requirements 4.8.
//
// For any queue name (with or without .fifo) and any dead-letter suffix, the dead-letter queue name is
// the queue name plus the suffix, with the suffix inserted before .fifo so .fifo stays last. The result
// ends in .fifo if and only if the input did. A result over 80 characters is never truncated: it throws
// ResourceNameTooLongException.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "2")]
public class SnsSqsDeadLetterQueueNamePropertyTests
{
    private const string FifoSuffix = ".fifo";

    // Feature: sns-sqs-provider, Property 2: Dead-letter queue name derivation
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsDeadLetterQueueNamePropertyTests) })]
    public Property DeadLetterQueueName_ForAnyQueueNameAndSuffix_InsertsSuffixBeforeFifoAndKeepsFifoLast(DlqCase c)
    {
        var queueName = c.Fifo ? c.Base + FifoSuffix : c.Base;
        var expected = c.Base + c.Suffix + (c.Fifo ? FifoSuffix : string.Empty);

        string? actual = null;
        var overflowed = false;
        try
        {
            actual = SnsSqsNamingConvention.DeadLetterQueueName(queueName, c.Suffix);
        }
        catch (ResourceNameTooLongException)
        {
            overflowed = true;
        }

        var ok = expected.Length > SnsSqsNamingConvention.QueueNameLimit
            ? overflowed
            : actual == expected
                && actual.EndsWith(FifoSuffix, StringComparison.Ordinal) == c.Fifo
                && actual.StartsWith(c.Base, StringComparison.Ordinal);

        return ok.ToProperty().Label($"queue='{queueName}' suffix='{c.Suffix}' result={actual ?? "overflow"}");
    }

    // ----- Generators -------------------------------------------------------------------------

    /// <summary>A generated queue name body, dead-letter suffix, and FIFO switch.</summary>
    public sealed record DlqCase(string Base, string Suffix, bool Fifo);

    public static Arbitrary<DlqCase> DlqCaseArbitrary() => Arb.From(DlqCaseGen());

    private static Gen<DlqCase> DlqCaseGen() =>
        from length in Gen.Elements(1, 5, 20, 50, 60, 70, 75, 80, 85)
        from chars in Gen.ArrayOf(NameCharGen(), length)
        from suffix in SuffixGen()
        from fifo in Gen.Elements(true, false)
        // The first character is alphanumeric so the queue name is never blank.
        select new DlqCase("q" + new string(chars)[1..], suffix, fifo);

    private static Gen<string> SuffixGen() =>
        Gen.OneOf(
            Gen.Constant("_error"),
            from length in Gen.Elements(0, 1, 6, 12, 30)
            from chars in Gen.ArrayOf(NameCharGen(), length)
            select new string(chars));

    private static Gen<char> NameCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".ToCharArray());
}
