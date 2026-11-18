namespace Messaging.MassTransit.AmazonSqs;

/// <summary>A subscription on an SNS topic, as reported by the control plane.</summary>
/// <param name="SubscriptionArn">The subscription ARN (may be <c>PendingConfirmation</c>).</param>
/// <param name="Protocol">The delivery protocol, for example <c>sqs</c>.</param>
/// <param name="Endpoint">The endpoint ARN the topic delivers to.</param>
public sealed record SnsSubscriptionInfo(string SubscriptionArn, string Protocol, string Endpoint);
