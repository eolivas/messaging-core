using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Consuming;
using Xunit;

namespace Messaging.Tests.Consuming;

// Unit tests for the consumer host lifecycle state sequence (task 9.5).
// Covers Req 3.3: a host advances Stopped -> Starting -> Running -> Stopping -> Stopped.
//
// ConsumerHostState has no distinct terminal Stopped member; Stopped is reused as both the initial
// and the terminal resting state, so the sequence begins and ends on Stopped. The externally
// observable milestones (via ConsumePipeline) are Stopped -> Running -> Stopped, because
// StartAsync/StopAsync are atomic from the caller's view. The intermediate Starting/Stopping states
// are asserted by driving ConsumerHostStateMachine directly.
[Trait("Feature", "messaging-core")]
public class ConsumerHostStateSequenceTests
{
    private static ConsumePipeline CreateHost() => new(
        new ConsumerHostStateMachine(),
        new LifecycleStubContractBinder(),
        new CountingHandlerInvoker());

    [Fact]
    public void State_BeforeStart_IsStopped()
    {
        var host = CreateHost();

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StartAsync_FromStopped_LeavesHostRunning()
    {
        var host = CreateHost();

        await host.StartAsync(CancellationToken.None);

        Assert.Equal(ConsumerHostState.Running, host.State);
    }

    [Fact]
    public async Task StopAsync_FromRunning_ReturnsHostToStopped()
    {
        var host = CreateHost();
        await host.StartAsync(CancellationToken.None);

        await host.StopAsync(CancellationToken.None);

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StartThenStop_ObservableMilestones_AreStoppedRunningStopped()
    {
        var host = CreateHost();
        var observed = new List<ConsumerHostState> { host.State };

        await host.StartAsync(CancellationToken.None);
        observed.Add(host.State);

        await host.StopAsync(CancellationToken.None);
        observed.Add(host.State);

        Assert.Equal(
            new[] { ConsumerHostState.Stopped, ConsumerHostState.Running, ConsumerHostState.Stopped },
            observed);
    }

    [Fact]
    public void StateMachine_SteppedThroughEachTransition_FollowsFullSequence()
    {
        var machine = new ConsumerHostStateMachine();
        var observed = new List<ConsumerHostState> { machine.State };

        machine.BeginStart();
        observed.Add(machine.State);

        machine.CompleteStart();
        observed.Add(machine.State);

        machine.BeginStop();
        observed.Add(machine.State);

        machine.CompleteStop();
        observed.Add(machine.State);

        Assert.Equal(
            new[]
            {
                ConsumerHostState.Stopped,
                ConsumerHostState.Starting,
                ConsumerHostState.Running,
                ConsumerHostState.Stopping,
                ConsumerHostState.Stopped,
            },
            observed);
    }

    [Fact]
    public async Task StopAsync_BeforeStart_ThrowsInvalidConsumerHostState()
    {
        var host = CreateHost();

        var exception = await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.StopAsync(CancellationToken.None));

        Assert.Equal(ConsumerHostState.Stopped, exception.CurrentState);
    }

    [Fact]
    public async Task StartAsync_WhenAlreadyRunning_ThrowsInvalidConsumerHostState()
    {
        var host = CreateHost();
        await host.StartAsync(CancellationToken.None);

        var exception = await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.StartAsync(CancellationToken.None));

        Assert.Equal(ConsumerHostState.Running, exception.CurrentState);
    }
}
