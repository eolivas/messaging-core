using Confluent.Kafka;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

public class KafkaClientConfigFactoryTests
{
    private static KafkaClientConfigFactory CreateFactory(KafkaSecuritySettings? security = null) =>
        new(KafkaTestSupport.ValidSettings(), security ?? new KafkaSecuritySettings());

    [Fact]
    public void CreateProducerConfig_WithDefaults_UsesAcksAllAndIdempotence()
    {
        var factory = CreateFactory();

        var config = factory.CreateProducerConfig();

        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void CreateConsumerConfig_WithDefaults_DisablesAutoCommitAndStartsAtEarliest()
    {
        var factory = CreateFactory();

        var config = factory.CreateConsumerConfig();

        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
        Assert.Equal("test-group", config.GroupId);
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void CreateProducerConfig_WithDefaultSecurity_IsPlaintextWithNoSaslOrTlsKeys()
    {
        var factory = CreateFactory();

        var config = factory.CreateProducerConfig();

        Assert.Equal(SecurityProtocol.Plaintext, config.SecurityProtocol);
        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SaslUsername);
        Assert.Null(config.SaslPassword);
        Assert.Null(config.SslCaLocation);
    }

    [Fact]
    public void CreateConsumerConfig_WithSaslSsl_MapsMechanismUsernameAndPassword()
    {
        var factory = CreateFactory(new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.SaslSsl,
            SaslMechanism = KafkaSaslMechanism.ScramSha512,
            SaslUsername = "user",
            SaslPassword = "pass",
        });

        var config = factory.CreateConsumerConfig();

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("user", config.SaslUsername);
        Assert.Equal("pass", config.SaslPassword);
    }

    [Fact]
    public void CreateProducerConfig_WithSslAndCaLocation_MapsCaLocation()
    {
        var factory = CreateFactory(new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.Ssl,
            TlsCaLocation = "/certs/ca.pem",
        });

        var config = factory.CreateProducerConfig();

        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Equal("/certs/ca.pem", config.SslCaLocation);
    }

    [Fact]
    public void CreateProducerConfig_WithSslAndNoCaLocation_LeavesCaLocationUnset()
    {
        var factory = CreateFactory(new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.Ssl,
        });

        var config = factory.CreateProducerConfig();

        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Null(config.SslCaLocation);
    }

    [Fact]
    public void CreateAdminClientConfig_WithSaslPlaintext_CarriesBootstrapServersAndSecurity()
    {
        var factory = CreateFactory(new KafkaSecuritySettings
        {
            SecurityProtocol = KafkaSecurityProtocol.SaslPlaintext,
            SaslMechanism = KafkaSaslMechanism.Plain,
            SaslUsername = "admin",
            SaslPassword = "secret",
        });

        var config = factory.CreateAdminClientConfig();

        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslPlaintext, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.Plain, config.SaslMechanism);
        Assert.Equal("admin", config.SaslUsername);
        Assert.Equal("secret", config.SaslPassword);
    }
}
