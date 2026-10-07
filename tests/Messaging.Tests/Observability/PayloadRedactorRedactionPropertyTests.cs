using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Observability;

namespace Messaging.Tests.Observability;

// Feature: messaging-core, Property 22: Redaction removes sensitive values before logging.
// Validates: Requirements 9.7.
//
// For an arbitrary set of structured log fields that mixes sensitive keys (drawn from the known
// sensitive-key set, in randomized casing to exercise the case-insensitive match) with
// non-sensitive keys, redacting the fields through PayloadRedactor.Redact(...) yields an output in
// which:
//   - every value under a sensitive key equals the fixed PayloadRedactor.RedactionMarker;
//   - every non-sensitive value is preserved unchanged;
//   - the original value of every sensitive field does NOT appear anywhere in the redacted output
//     (neither as a whole value nor as a substring of any emitted value), so a secret cannot leak
//     into a written log entry — Req 9.7.
//
// To make the "does not appear" check unambiguous, each generated sensitive value is a unique
// unguessable token (so it cannot coincidentally equal a non-sensitive value), while the generator
// still forces adversarial overlap by letting non-sensitive values carry fragments of the marker
// and arbitrary text. The source dictionary is also asserted to be left unmutated.
[Trait("Feature", "messaging-core")]
[Trait("Property", "22")]
public class PayloadRedactorRedactionPropertyTests
{
    /// <summary>Key names the default redactor treats as sensitive (Req 9.7).</summary>
    private static readonly string[] KnownSensitiveKeys =
    {
        "password", "passwd", "pwd", "secret", "token", "accesstoken", "access_token",
        "refreshtoken", "refresh_token", "apikey", "api_key", "authorization", "auth",
        "credential", "credentials", "privatekey", "private_key", "connectionstring",
        "connection_string", "ssn", "creditcard", "credit_card", "cardnumber", "card_number",
        "cvv", "pin",
    };

    /// <summary>A single field: its key, its original value, and whether that key is sensitive.</summary>
    public sealed record Field(string Key, string Value, bool Sensitive);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(PayloadRedactorRedactionPropertyTests) })]
    public Property Redact_FieldsWithSensitiveKeys_ReplacesSensitiveValuesAndPreservesTheRest(
        IReadOnlyList<Field> fields)
    {
        // Arrange: the default redactor, and a source map built from the generated fields. Keys are
        // generated distinct, so no entry is clobbered and every original field survives into input.
        var redactor = new PayloadRedactor();
        var input = fields.ToDictionary(f => f.Key, f => f.Value);
        var inputSnapshot = new Dictionary<string, string>(input);

        // Act.
        var redacted = redactor.Redact(input);

        // Assert.
        var sameShape = redacted.Count == input.Count;

        var sensitiveRedacted = fields
            .Where(f => f.Sensitive)
            .All(f => redacted.TryGetValue(f.Key, out var value)
                && value == PayloadRedactor.RedactionMarker);

        var nonSensitivePreserved = fields
            .Where(f => !f.Sensitive)
            .All(f => redacted.TryGetValue(f.Key, out var value) && value == f.Value);

        // No original sensitive value may surface anywhere in the redacted output — not as a whole
        // value and not as a substring of any emitted value.
        var sensitiveOriginals = fields
            .Where(f => f.Sensitive)
            .Select(f => f.Value)
            .Where(v => v.Length > 0)
            .ToArray();

        var noLeak = sensitiveOriginals.All(secret =>
            redacted.Values.All(emitted => !emitted.Contains(secret, StringComparison.Ordinal)));

        // The source map must not be mutated by redaction.
        var sourceUntouched =
            input.Count == inputSnapshot.Count
            && inputSnapshot.All(kvp => input.TryGetValue(kvp.Key, out var v) && v == kvp.Value);

        var holds = sameShape && sensitiveRedacted && nonSensitivePreserved && noLeak && sourceUntouched;

        var sensitiveCount = fields.Count(f => f.Sensitive);

        return holds
            .ToProperty()
            .Classify(fields.Count == 0, "empty field set")
            .Classify(sensitiveCount == 0, "no sensitive fields")
            .Classify(sensitiveCount == fields.Count && fields.Count > 0, "all sensitive")
            .Label($"fields={fields.Count} sensitive={sensitiveCount}");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<IReadOnlyList<Field>> FieldsArbitrary() =>
        Arb.From(FieldsGen());

    /// <summary>
    /// Generates 0..24 fields with distinct keys. Each field is either sensitive (key drawn from the
    /// known set in randomized casing, value a unique unguessable token) or non-sensitive (a key that
    /// is not in the sensitive set, value an arbitrary string that may carry marker fragments to
    /// probe for false negatives).
    /// </summary>
    private static Gen<IReadOnlyList<Field>> FieldsGen() =>
        from count in Gen.Choose(0, 24)
        from fields in GenDistinctKeyedFields(count)
        select (IReadOnlyList<Field>)fields;

    private static Gen<List<Field>> GenDistinctKeyedFields(int count) =>
        Gen.ArrayOf(FieldGen(), count).Select(generated =>
        {
            // Keep the first field per distinct (case-insensitive) key so the source map never
            // collides, while preserving each kept field's Sensitive flag and value.
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var kept = new List<Field>(generated.Length);
            foreach (var field in generated)
            {
                if (seen.Add(field.Key))
                {
                    kept.Add(field);
                }
            }

            return kept;
        });

    private static Gen<Field> FieldGen() =>
        Gen.OneOf(SensitiveFieldGen(), NonSensitiveFieldGen());

    /// <summary>
    /// A sensitive field: a known-sensitive key name in randomized casing (to exercise the
    /// case-insensitive match) paired with a unique token value that cannot coincide with any other
    /// generated value, so a leak is unambiguous.
    /// </summary>
    private static Gen<Field> SensitiveFieldGen() =>
        from key in Gen.Elements(KnownSensitiveKeys)
        from cased in RandomCase(key)
        select new Field(cased, "SECRET-" + Guid.NewGuid().ToString("N"), Sensitive: true);

    /// <summary>
    /// A non-sensitive field: a key that is not in the sensitive set, paired with an arbitrary value
    /// that may embed fragments of the redaction marker to probe for false negatives.
    /// </summary>
    private static Gen<Field> NonSensitiveFieldGen() =>
        from suffix in TokenGen()
        from value in NonSensitiveValueGen()
        select new Field("field-" + suffix, value, Sensitive: false);

    private static Gen<string> NonSensitiveValueGen() =>
        Gen.OneOf(
            TokenGen(),
            Gen.Constant(string.Empty),
            Gen.Constant("REDACTED"),
            Gen.Constant("***"),
            Gen.Constant(PayloadRedactor.RedactionMarker));

    /// <summary>Flips each character of <paramref name="value"/> to a random case.</summary>
    private static Gen<string> RandomCase(string value) =>
        from flips in Gen.ArrayOf(Gen.Elements(true, false), value.Length)
        select new string(value
            .Select((c, i) => flips[i] ? char.ToUpperInvariant(c) : char.ToLowerInvariant(c))
            .ToArray());

    private static Gen<string> TokenGen() =>
        from length in Gen.Choose(1, 16)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.".ToCharArray());
}
