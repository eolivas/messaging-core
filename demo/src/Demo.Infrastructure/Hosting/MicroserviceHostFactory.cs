using Demo.Application.Sessions;
using Demo.Communications;
using Demo.Contracts;
using Demo.Customer.Application;
using Demo.Infrastructure.Composition;
using Demo.Infrastructure.Messaging;
using Demo.SalesLeads.Application;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Observability;
using Messaging.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Hosting;

/// <summary>
/// Registers the demo handler chain around a provider composition. Consumer chain, outermost
/// first: contract validation, flow reporting, failure simulation, the real handler. Each host
/// owns its own idempotency store, attempt tracker and dead-letter sink.
/// </summary>
public sealed class MicroserviceHostFactory : IMicroserviceHostFactory
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly IFlowStepSink _sink;
    private readonly TimeProvider _time;

    public MicroserviceHostFactory(ILoggerFactory loggerFactory, IFlowStepSink sink, TimeProvider time)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _time = time ?? throw new ArgumentNullException(nameof(time));
    }

    public IMicroserviceHost Create(
        MicroserviceId service,
        IProviderComposition composition,
        ClusterStartRequest request,
        Func<FailureMode> failureMode)
    {
        ArgumentNullException.ThrowIfNull(composition);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(failureMode);

        var context = new CompositionContext(service, request.EffectiveSettings);
        return MicroserviceHost.Create(service, _loggerFactory, services =>
        {
            services.AddSingleton(_sink);
            services.AddSingleton(_time);
            services.AddSingleton<IMessagingLog>(sp => new DemoMessagingLog(
                service, QuoteAcceptedEvent.MessageType, _sink, sp.GetRequiredService<ILogger<DemoMessagingLog>>()));

            if (service == MicroserviceId.SalesLeads)
            {
                services.AddSingleton<IMockDataGenerator, MockDataGenerator>();
                services.AddSingleton<ISalesLeadsService>(sp => new SalesLeadsService(
                    sp.GetRequiredService<IMessagePublisher>(),
                    sp.GetRequiredService<IMockDataGenerator>(),
                    _sink,
                    _time));
                composition.ConfigurePublisher(services, context);
                return;
            }

            services.AddSingleton(failureMode);
            ConfigureConsumerServices(services, service);
            composition.ConfigureConsumer(services, IdentityOf(service), context);
        });
    }

    private void ConfigureConsumerServices(IServiceCollection services, MicroserviceId service)
    {
        services.AddSingleton<AttemptTracker>();
        services.AddSingleton(new ContractRegistration(QuoteAcceptedEvent.MessageType, typeof(QuoteAcceptedEvent)));
        services.AddSingleton<IIdempotencyStore>(_ => new InMemoryIdempotencyStore(_time));
        services.AddSingleton<InMemoryDeadLetterSink>();
        // TryAdd: the Amazon composition registers its own sink afterwards, which then wins.
        services.TryAddSingleton<IDeadLetterSink>(sp => sp.GetRequiredService<InMemoryDeadLetterSink>());
        services.AddSingleton<IDeadLetterLog>(sp => new DemoDeadLetterLog(
            service,
            sp.GetRequiredService<AttemptTracker>(),
            _sink,
            sp.GetRequiredService<ILogger<DemoDeadLetterLog>>(),
            _time));
        services.AddSingleton<IMessageHandler<QuoteAcceptedEvent>>(sp => BuildChain(sp, service));
        services.AddSingleton<IHandlerInvoker>(sp => new QuoteAcceptedHandlerInvoker(
            sp.GetRequiredService<IMessageHandler<QuoteAcceptedEvent>>()));
    }

    private IMessageHandler<QuoteAcceptedEvent> BuildChain(IServiceProvider sp, MicroserviceId service)
    {
        IMessageHandler<QuoteAcceptedEvent> real = service == MicroserviceId.Customer
            ? new CreateCustomerHandler(new InMemoryCustomerStore(), _sink, _time)
            : new SendQuoteEmailHandler(new InMemoryMockEmailOutbox(), _sink, _time);
        var tracker = sp.GetRequiredService<AttemptTracker>();
        var mode = sp.GetRequiredService<Func<FailureMode>>();
        var simulating = new FailureSimulatingHandler(real, mode, tracker);
        return new FlowReportingHandler(
            service,
            simulating,
            new FailureClassifier(),
            tracker,
            _sink,
            sp.GetRequiredService<MessagingConfiguration>().Retry.MaxAttempts,
            _time);
    }

    private static ConsumerIdentity IdentityOf(MicroserviceId service) =>
        service == MicroserviceId.Customer ? ConsumerIdentity.Customer : ConsumerIdentity.Communications;
}
