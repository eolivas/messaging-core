namespace Demo.Infrastructure.Hosting;

/// <summary>A broker endpoint a provider needs, and the command that starts it.</summary>
public sealed record BrokerEndpoint(string ProviderName, string Host, int Port, string StartCommand);
