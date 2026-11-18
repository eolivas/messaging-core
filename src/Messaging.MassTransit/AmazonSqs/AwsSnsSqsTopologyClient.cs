using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// <see cref="ISnsSqsTopologyClient"/> over the AWS SDK. The only code that touches the SDK control plane.
/// Only "not found" is translated (to null or empty); every other SDK error propagates to the caller.
/// </summary>
public sealed class AwsSnsSqsTopologyClient : ISnsSqsTopologyClient
{
    private readonly IAmazonSQS _sqs;
    private readonly IAmazonSimpleNotificationService _sns;

    /// <summary>Creates the client over the given SDK clients.</summary>
    public AwsSnsSqsTopologyClient(IAmazonSQS sqs, IAmazonSimpleNotificationService sns)
    {
        _sqs = sqs ?? throw new ArgumentNullException(nameof(sqs));
        _sns = sns ?? throw new ArgumentNullException(nameof(sns));
    }

    /// <inheritdoc />
    public async Task<string?> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken)
    {
        try
        {
            var response = await _sqs.GetQueueUrlAsync(
                new GetQueueUrlRequest { QueueName = queueName }, cancellationToken).ConfigureAwait(false);
            return response.QueueUrl;
        }
        catch (QueueDoesNotExistException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>?> GetQueueAttributesAsync(
        string queueUrl, CancellationToken cancellationToken)
    {
        try
        {
            var response = await _sqs.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = new List<string> { "All" },
                },
                cancellationToken).ConfigureAwait(false);
            return new Dictionary<string, string>(response.Attributes ?? new Dictionary<string, string>());
        }
        catch (QueueDoesNotExistException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<string> CreateQueueAsync(
        string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        var response = await _sqs.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = new Dictionary<string, string>(attributes) },
            cancellationToken).ConfigureAwait(false);
        return response.QueueUrl;
    }

    /// <inheritdoc />
    public async Task SetQueueAttributesAsync(
        string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        await _sqs.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string>(attributes),
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<string?> GetTopicArnAsync(string topicName, CancellationToken cancellationToken)
    {
        // SNS has no get-by-name without the account id, so page ListTopics and match the ARN suffix.
        var suffix = ":" + topicName;
        string? nextToken = null;
        do
        {
            var response = await _sns.ListTopicsAsync(
                new ListTopicsRequest { NextToken = nextToken }, cancellationToken).ConfigureAwait(false);
            var match = response.Topics?.FirstOrDefault(
                t => t.TopicArn.EndsWith(suffix, StringComparison.Ordinal));
            if (match is not null)
            {
                return match.TopicArn;
            }

            nextToken = response.NextToken;
        }
        while (!string.IsNullOrEmpty(nextToken));

        return null;
    }

    /// <inheritdoc />
    public async Task<string> CreateTopicAsync(
        string topicName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken)
    {
        var response = await _sns.CreateTopicAsync(
            new CreateTopicRequest { Name = topicName, Attributes = new Dictionary<string, string>(attributes) },
            cancellationToken).ConfigureAwait(false);
        return response.TopicArn;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<SnsSubscriptionInfo>> ListSubscriptionsByTopicAsync(
        string topicArn, CancellationToken cancellationToken)
    {
        var result = new List<SnsSubscriptionInfo>();
        string? nextToken = null;
        try
        {
            do
            {
                var response = await _sns.ListSubscriptionsByTopicAsync(
                    new ListSubscriptionsByTopicRequest { TopicArn = topicArn, NextToken = nextToken },
                    cancellationToken).ConfigureAwait(false);
                if (response.Subscriptions is not null)
                {
                    result.AddRange(response.Subscriptions.Select(
                        s => new SnsSubscriptionInfo(s.SubscriptionArn, s.Protocol, s.Endpoint)));
                }

                nextToken = response.NextToken;
            }
            while (!string.IsNullOrEmpty(nextToken));
        }
        catch (NotFoundException)
        {
            return Array.Empty<SnsSubscriptionInfo>();
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<string> SubscribeAsync(
        string topicArn,
        string queueArn,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken)
    {
        var response = await _sns.SubscribeAsync(
            new SubscribeRequest
            {
                TopicArn = topicArn,
                Protocol = "sqs",
                Endpoint = queueArn,
                Attributes = new Dictionary<string, string>(attributes),
                ReturnSubscriptionArn = true,
            },
            cancellationToken).ConfigureAwait(false);
        return response.SubscriptionArn;
    }

    /// <inheritdoc />
    public async Task PingAsync(CancellationToken cancellationToken)
    {
        await _sqs.ListQueuesAsync(new ListQueuesRequest { MaxResults = 1 }, cancellationToken)
            .ConfigureAwait(false);
        await _sns.ListTopicsAsync(new ListTopicsRequest(), cancellationToken).ConfigureAwait(false);
    }
}
