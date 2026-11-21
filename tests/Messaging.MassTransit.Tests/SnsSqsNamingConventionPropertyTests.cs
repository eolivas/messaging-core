using System;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.MassTransit.AmazonSqs;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Feature: sns-sqs-provider, Property 1: Topic and queue name derivation
// Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7.
//
// For any message type string or endpoint name, any prefix, and either FIFO mode, the derived name is
// the MassTransit-style formatted subject with the prefix prepended, contains only [A-Za-z0-9_-] (plus a
// trailing .fifo in FIFO mode), is within 256 (topic) / 80 (queue) characters, and is never truncated:
// an overflow throws ResourceNameTooLongException. Deriving twice yields identical results.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "1")]
public class SnsSqsNamingConventionPropertyTests
{
    private const string FifoSuffix = ".fifo";

    // Feature: sns-sqs-provider, Property 1: Topic and queue name derivation
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsNamingConventionPropertyTests) })]
    public Property TopicName_ForAnyInput_IsFormattedPrefixedValidBoundedAndIdempotent(NameCase c)
    {
        var settings = c.ToSettings();
        var first = Derive(() => SnsSqsNamingConvention.TopicName(c.Subject, settings));
        var second = Derive(() => SnsSqsNamingConvention.TopicName(c.Subject, settings));

        var expectedBody = ExpectedTopicBody(c.Subject);
        var expected = c.Prefix + expectedBody + (c.Fifo ? FifoSuffix : string.Empty);
        var mustOverflow = expected.Length > SnsSqsNamingConvention.TopicNameLimit;

        var ok = Check(first, second, expected, mustOverflow, SnsSqsNamingConvention.TopicNameLimit, c)
            && (first.Name is null || first.Name == expected);

        return ok.ToProperty().Label($"subject='{c.Subject}' prefix='{c.Prefix}' fifo={c.Fifo} result={first.Name ?? first.Error}");
    }

    // Feature: sns-sqs-provider, Property 1: Topic and queue name derivation
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsNamingConventionPropertyTests) })]
    public Property QueueName_ForAnyInput_IsPrefixedValidBoundedAndIdempotent(NameCase c)
    {
        var settings = c.ToSettings();
        var first = Derive(() => SnsSqsNamingConvention.QueueName(c.Subject, settings));
        var second = Derive(() => SnsSqsNamingConvention.QueueName(c.Subject, settings));

        // Kebab-casing and sanitizing never shorten the subject, so prefix + subject (+ .fifo) is a lower bound.
        var lowerBound = c.Prefix.Length + c.Subject.Length + (c.Fifo ? FifoSuffix.Length : 0);
        var mustOverflow = lowerBound > SnsSqsNamingConvention.QueueNameLimit;

        var ok = Check(first, second, null, mustOverflow, SnsSqsNamingConvention.QueueNameLimit, c);
        if (ok && first.Name is not null)
        {
            var body = first.Name[c.Prefix.Length..];
            body = c.Fifo ? body[..^FifoSuffix.Length] : body;
            ok = body.All(ch => !char.IsUpper(ch));
        }

        return ok.ToProperty().Label($"subject='{c.Subject}' prefix='{c.Prefix}' fifo={c.Fifo} result={first.Name ?? first.Error}");
    }

    // ----- Oracle -----------------------------------------------------------------------------

    private static bool Check(Outcome first, Outcome second, string? expected, bool mustOverflow, int limit, NameCase c)
    {
        if (first != second)
        {
            return false; // idempotence
        }

        if (first.Name is null)
        {
            // Overflow must be reported with the limit; the name is never truncated.
            return first.Limit == limit && (expected is null || expected.Length > limit);
        }

        var name = first.Name;
        if (mustOverflow || name.Length > limit || !name.StartsWith(c.Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var body = name;
        if (c.Fifo)
        {
            if (!name.EndsWith(FifoSuffix, StringComparison.Ordinal))
            {
                return false;
            }

            body = name[..^FifoSuffix.Length];
        }

        return body.All(IsPermitted);
    }

    // MassTransit style: namespace dots become '_', namespace and type joined by '-', nested '+' becomes '_'.
    private static string ExpectedTopicBody(string messageType)
    {
        var split = messageType.LastIndexOf('.');
        var type = (split < 0 ? messageType : messageType[(split + 1)..]).Replace('+', '_');
        var formatted = split < 0 ? type : messageType[..split].Replace('.', '_') + "-" + type;
        return new string(formatted.Select(ch => IsPermitted(ch) ? ch : '_').ToArray());
    }

    private static bool IsPermitted(char ch) =>
        ch is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '-';

    private static Outcome Derive(Func<string> derive)
    {
        try
        {
            return new Outcome(derive(), null, 0);
        }
        catch (ResourceNameTooLongException ex)
        {
            return new Outcome(null, ex.Subject, ex.Limit);
        }
    }

    private sealed record Outcome(string? Name, string? Error, int Limit);

    // ----- Generators -------------------------------------------------------------------------

    /// <summary>A generated subject (message type or endpoint name), prefix, and FIFO switch.</summary>
    public sealed record NameCase(string Subject, string Prefix, bool Fifo)
    {
        public AmazonSqsProviderSettings ToSettings() => new()
        {
            TopicNamePrefix = Prefix,
            QueueNamePrefix = Prefix,
            Fifo = Fifo,
        };
    }

    public static Arbitrary<NameCase> NameCaseArbitrary() => Arb.From(NameCaseGen());

    private static Gen<NameCase> NameCaseGen() =>
        from length in Gen.Elements(1, 5, 20, 60, 75, 80, 81, 100, 250, 256, 260, 300)
        from chars in Gen.ArrayOf(SubjectCharGen(), length)
        from prefixLength in Gen.Elements(0, 0, 3, 10, 40)
        from prefixChars in Gen.ArrayOf(PrefixCharGen(), prefixLength)
        from fifo in Gen.Elements(true, false)
        // The first character is alphanumeric so the subject is never blank.
        select new NameCase("A" + new string(chars)[1..], new string(prefixChars), fifo);

    private static Gen<char> SubjectCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_..+ /:".ToCharArray());

    private static Gen<char> PrefixCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".ToCharArray());
}
