using System;
using System.Linq;
using System.Reflection;
using Messaging.MassTransit.AmazonSqs;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 4.1, 4.3, 4.4, 4.5, 4.6, 17.2.
[Trait("Feature", "sns-sqs-provider")]
public class SnsSqsNamingConventionTests
{
    public sealed class Plain
    {
    }

    public sealed class Nested
    {
        public sealed class Inner
        {
        }
    }

    private static AmazonSqsProviderSettings Settings(
        string topicPrefix = "", string queuePrefix = "", bool fifo = false) => new()
    {
        TopicNamePrefix = topicPrefix,
        QueueNamePrefix = queuePrefix,
        Fifo = fifo,
    };

    [Fact]
    public void TopicName_WithPrefix_PrependsPrefix()
    {
        var name = SnsSqsNamingConvention.TopicName("Acme.Orders.OrderPlaced", Settings(topicPrefix: "dev-"));

        Assert.Equal("dev-Acme_Orders-OrderPlaced", name);
    }

    [Fact]
    public void QueueName_WithPrefix_PrependsPrefixToKebabCaseName()
    {
        var name = SnsSqsNamingConvention.QueueName("OrderService", Settings(queuePrefix: "dev-"));

        Assert.Equal("dev-order-service", name);
    }

    [Fact]
    public void TopicName_WithoutPrefix_HasNoPrefix()
    {
        Assert.Equal("Acme-Evt", SnsSqsNamingConvention.TopicName("Acme.Evt", Settings()));
    }

    [Theory]
    [InlineData("Acme.Evt+Inner", "Acme-Evt_Inner")]
    [InlineData("Acme.Evt`1", "Acme-Evt_1")]
    [InlineData("Acme.Evt/Sub:1", "Acme-Evt_Sub_1")]
    public void TopicName_WithInvalidCharacters_ReplacesThemWithUnderscore(string type, string expected)
    {
        Assert.Equal(expected, SnsSqsNamingConvention.TopicName(type, Settings()));
    }

    [Fact]
    public void QueueName_WithInvalidCharacters_ReplacesThemWithUnderscore()
    {
        Assert.Equal("my_queue_name", SnsSqsNamingConvention.QueueName("my queue/name", Settings()));
    }

    [Fact]
    public void TopicName_AtLimit_Succeeds()
    {
        var type = new string('a', SnsSqsNamingConvention.TopicNameLimit);

        Assert.Equal(type, SnsSqsNamingConvention.TopicName(type, Settings()));
    }

    [Fact]
    public void TopicName_OverLimit_ThrowsWithoutTruncating()
    {
        var type = new string('a', SnsSqsNamingConvention.TopicNameLimit + 1);

        var ex = Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.TopicName(type, Settings()));

        Assert.Equal(256, ex.Limit);
        Assert.Equal(type, ex.Subject);
    }

    [Fact]
    public void TopicName_FifoSuffixPushesOverLimit_Throws()
    {
        var type = new string('a', SnsSqsNamingConvention.TopicNameLimit);

        Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.TopicName(type, Settings(fifo: true)));
    }

    [Fact]
    public void QueueName_AtLimit_Succeeds()
    {
        var endpoint = new string('a', SnsSqsNamingConvention.QueueNameLimit);

        Assert.Equal(endpoint, SnsSqsNamingConvention.QueueName(endpoint, Settings()));
    }

    [Fact]
    public void QueueName_OverLimit_ThrowsWithoutTruncating()
    {
        var endpoint = new string('a', SnsSqsNamingConvention.QueueNameLimit + 1);

        var ex = Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.QueueName(endpoint, Settings()));

        Assert.Equal(80, ex.Limit);
    }

    [Fact]
    public void QueueName_PrefixPushesOverLimit_Throws()
    {
        var endpoint = new string('a', SnsSqsNamingConvention.QueueNameLimit);

        Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.QueueName(endpoint, Settings(queuePrefix: "p-")));
    }

    [Fact]
    public void TopicName_Fifo_AppendsFifoSuffix()
    {
        Assert.Equal("p-Acme-Evt.fifo", SnsSqsNamingConvention.TopicName("Acme.Evt", Settings("p-", fifo: true)));
    }

    [Fact]
    public void QueueName_Fifo_AppendsFifoSuffix()
    {
        Assert.Equal("order-service.fifo", SnsSqsNamingConvention.QueueName("OrderService", Settings(fifo: true)));
    }

    [Fact]
    public void QueueName_NotFifo_HasNoFifoSuffix()
    {
        Assert.DoesNotContain(".fifo", SnsSqsNamingConvention.QueueName("OrderService", Settings()));
    }

    [Fact]
    public void QueueName_EndpointAlreadyEndsWithFifo_DoesNotDoubleSuffix()
    {
        Assert.Equal("orders.fifo", SnsSqsNamingConvention.QueueName("orders.fifo", Settings(fifo: true)));
    }

    [Fact]
    public void DeadLetterQueueName_Standard_AppendsSuffix()
    {
        Assert.Equal("orders_error", SnsSqsNamingConvention.DeadLetterQueueName("orders", "_error"));
    }

    [Fact]
    public void DeadLetterQueueName_Fifo_InsertsSuffixBeforeFifo()
    {
        Assert.Equal("orders_error.fifo", SnsSqsNamingConvention.DeadLetterQueueName("orders.fifo", "_error"));
    }

    [Fact]
    public void DeadLetterQueueName_OverLimit_Throws()
    {
        var queue = new string('a', SnsSqsNamingConvention.QueueNameLimit);

        var ex = Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.DeadLetterQueueName(queue, "_error"));

        Assert.Equal(80, ex.Limit);
    }

    [Fact]
    public void DeadLetterQueueName_FifoSuffixCountsTowardLimit_Throws()
    {
        var queue = new string('a', SnsSqsNamingConvention.QueueNameLimit - 5) + ".fifo";

        Assert.Throws<ResourceNameTooLongException>(
            () => SnsSqsNamingConvention.DeadLetterQueueName(queue, "_error"));
    }

    [Theory]
    [InlineData(typeof(Plain))]
    [InlineData(typeof(Nested.Inner))]
    [InlineData(typeof(SnsSqsNamingConventionTests))]
    [InlineData(typeof(ResourceNameTooLongException))]
    public void TopicName_ForClrType_MatchesMassTransitMessageNameFormatter(Type type)
    {
        // The formatter reads Type.FullName; the convention takes that string.
        Assert.NotNull(type.FullName);
        var expected = MassTransitMessageName(type);

        var actual = SnsSqsNamingConvention.TopicName(type.FullName!, Settings());

        Assert.Equal(expected, actual);
    }

    // The MassTransit formatter is not part of the public surface in every build, so it is reached by reflection.
    private static string MassTransitMessageName(Type type)
    {
        var formatterType = Assembly.Load("MassTransit.AmazonSqsTransport")
            .GetTypes()
            .Single(t => t.Name == "AmazonSqsMessageNameFormatter");
        var formatter = Activator.CreateInstance(
            formatterType, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.OptionalParamBinding,
            null, new object[] { Type.Missing, Type.Missing, Type.Missing, Type.Missing }, null)!;
        var method = formatterType.GetMethod("GetMessageName", BindingFlags.Public | BindingFlags.Instance)!;
        return (string)method.Invoke(formatter, new object[] { type })!;
    }
}
