using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Pure merge of the SNS-to-SQS access policy on a queue. Statements it does not own are kept;
/// each topic gets exactly one <c>Allow sqs:SendMessage</c> statement for the SNS service principal,
/// scoped by an <c>ArnEquals</c> <c>aws:SourceArn</c> condition. A wildcard principal is never emitted.
/// </summary>
public static class QueuePolicyDocument
{
    private const string PolicyVersion = "2012-10-17";
    private const string SidPrefix = "AllowSnsTopic";

    private static readonly JsonSerializerOptions WriteOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        WriteIndented = false,
    };

    /// <summary>Returns the policy JSON with one SNS statement per topic ARN.</summary>
    /// <param name="existingPolicyJson">The queue's current policy JSON, or null/blank when none.</param>
    /// <param name="queueArn">The ARN of the queue the policy is attached to.</param>
    /// <param name="topicArns">The topic ARNs allowed to send to the queue.</param>
    /// <returns>The merged policy JSON. Merging the result again with the same inputs returns it unchanged.</returns>
    /// <exception cref="ArgumentException">If an ARN is null or blank, or the existing policy is not a valid policy document.</exception>
    public static string Merge(string? existingPolicyJson, string queueArn, IEnumerable<string> topicArns)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueArn);
        ArgumentNullException.ThrowIfNull(topicArns);

        var topics = topicArns.ToList();
        if (topics.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("Topic ARNs must not be null or blank.", nameof(topicArns));
        }

        var root = ParseExisting(existingPolicyJson);
        var statements = ReadStatements(root);

        foreach (var topicArn in topics.Distinct(StringComparer.Ordinal).OrderBy(t => t, StringComparer.Ordinal))
        {
            var sid = SidFor(topicArn);
            var statement = BuildStatement(sid, queueArn, topicArn);
            var index = IndexOfSid(statements, sid);

            if (index >= 0)
            {
                statements[index] = statement;
                RemoveOtherSid(statements, sid, index);
            }
            else
            {
                statements.Add(statement);
            }
        }

        root["Version"] ??= PolicyVersion;
        root["Statement"] = statements;
        return root.ToJsonString(WriteOptions);
    }

    /// <summary>Returns the deterministic statement id for a topic ARN.</summary>
    /// <param name="topicArn">The topic ARN.</param>
    /// <returns>An alphanumeric id derived from the SHA-256 hash of the ARN.</returns>
    public static string SidFor(string topicArn)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicArn);
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(topicArn));
        return SidPrefix + Convert.ToHexString(hash, 0, 16);
    }

    private static JsonObject ParseExisting(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return new JsonObject { ["Version"] = PolicyVersion };
        }

        try
        {
            return JsonNode.Parse(json) as JsonObject
                ?? throw new ArgumentException("Existing queue policy must be a JSON object.", nameof(json));
        }
        catch (JsonException ex)
        {
            throw new ArgumentException("Existing queue policy is not valid JSON.", nameof(json), ex);
        }
    }

    private static JsonArray ReadStatements(JsonObject root)
    {
        var array = new JsonArray();
        switch (root["Statement"])
        {
            case null:
                break;
            case JsonArray existing:
                foreach (var item in existing)
                {
                    array.Add(item?.DeepClone());
                }

                break;
            case JsonObject single:
                array.Add(single.DeepClone());
                break;
            default:
                throw new ArgumentException("Existing queue policy has an invalid Statement element.");
        }

        return array;
    }

    private static int IndexOfSid(JsonArray statements, string sid)
    {
        for (var i = 0; i < statements.Count; i++)
        {
            if (HasSid(statements[i], sid))
            {
                return i;
            }
        }

        return -1;
    }

    private static void RemoveOtherSid(JsonArray statements, string sid, int keep)
    {
        for (var i = statements.Count - 1; i >= 0; i--)
        {
            if (i != keep && HasSid(statements[i], sid))
            {
                statements.RemoveAt(i);
            }
        }
    }

    private static bool HasSid(JsonNode? statement, string sid) =>
        statement is JsonObject obj
        && obj["Sid"] is JsonValue value
        && value.TryGetValue<string>(out var existing)
        && string.Equals(existing, sid, StringComparison.Ordinal);

    private static JsonObject BuildStatement(string sid, string queueArn, string topicArn) => new()
    {
        ["Sid"] = sid,
        ["Effect"] = "Allow",
        ["Principal"] = new JsonObject { ["Service"] = "sns.amazonaws.com" },
        ["Action"] = "sqs:SendMessage",
        ["Resource"] = queueArn,
        ["Condition"] = new JsonObject
        {
            ["ArnEquals"] = new JsonObject { ["aws:SourceArn"] = topicArn },
        },
    };
}
