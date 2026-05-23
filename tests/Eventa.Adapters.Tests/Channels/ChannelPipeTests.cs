using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

using Eventa.Adapters.Channels;

using ChannelClosedException = Eventa.Adapters.Channels.ChannelClosedException;

namespace Eventa.Adapters.Tests.Channels;

public class ChannelPipeTests
{
    [Fact]
    public async Task Emit_ForwardsEventToPairedEndpoint()
    {
        using var pipe = new ChannelPipe();
        var definition = new EventDefinition<TestPayload>("channel:event");
        var received = new TaskCompletionSource<EventEnvelope<TestPayload>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = pipe.Right.Subscribe(definition, envelope => received.TrySetResult(envelope));

        pipe.Left.Emit(definition, new TestPayload("hello"));

        var envelope = await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(new EventEnvelope<TestPayload>("channel:event", new TestPayload("hello")), envelope);
    }

    [Fact]
    public async Task InvokeAsync_ForwardsUnaryInvokeAcrossPipe()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<EchoResponse, EchoRequest>("channel:echo");

        using var _ = pipe.Right.RegisterInvokeHandler(
            definition,
            static (request, _) => Task.FromResult(new EchoResponse(request.Value.ToUpperInvariant())));

        var client = pipe.Left.CreateInvokeClient(definition);
        var response = await client.InvokeAsync(new EchoRequest("eventa"), TestContext.Current.CancellationToken);

        Assert.Equal(new EchoResponse("EVENTA"), response);
    }

    [Fact]
    public async Task InvokeAsync_ForwardsRequestStreamUnaryInvokeAcrossPipe()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<int, int>("channel:sum");

        using var _ = pipe.Right.RegisterInvokeHandler(
            definition,
            static async (request, cancellationToken) =>
            {
                var sum = 0;
                await foreach (var value in request.WithCancellation(cancellationToken))
                {
                    sum += value;
                }

                return sum;
            });

        var client = pipe.Left.CreateInvokeClient(definition);
        var response = await client.InvokeAsync(Numbers(1, 2, 3), TestContext.Current.CancellationToken);

        Assert.Equal(6, response);
    }

    [Fact]
    public async Task InvokeStreamAsync_ForwardsServerStreamAcrossPipe()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<int, int>("channel:count");

        using var _ = pipe.Right.RegisterStreamHandler(definition, CountAsync);

        var client = pipe.Left.CreateInvokeStreamClient(definition);
        var responses = await CollectAsync(client.InvokeAsync(3, TestContext.Current.CancellationToken));

        Assert.Equal([1, 2, 3], responses);
    }

    [Fact]
    public async Task InvokeStreamAsync_ForwardsBidirectionalStreamAcrossPipe()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<int, int>("channel:bidi");

        using var _ = pipe.Right.RegisterStreamHandler(definition, DoubleAsync);

        var client = pipe.Left.CreateInvokeStreamClient(definition);
        var responses = await CollectAsync(client.InvokeAsync(Numbers(1, 2, 3), TestContext.Current.CancellationToken));

        Assert.Equal([2, 4, 6], responses);
    }

    private static async IAsyncEnumerable<int> DoubleAsync(
        IAsyncEnumerable<int> request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var value in request.WithCancellation(cancellationToken))
        {
            yield return value * 2;
        }
    }

    [Fact]
    public async Task Dispose_FaultsPendingUnaryInvokeBeforeClosedEvent()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<string, string>("channel:dispose-pending");
        var closedObserved = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = pipe.Left.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        using var _ = pipe.Left.Subscribe(ChannelEvents.Closed, _ =>
        {
            if (pending.IsFaulted)
            {
                closedObserved.TrySetResult(true);
            }
            else
            {
                closedObserved.TrySetException(new InvalidOperationException("Closed event ran before invoke faulted."));
            }
        });

        pipe.Left.Dispose();

        var error = await Assert.ThrowsAsync<ChannelClosedException>(
            async () => await pending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        await closedObserved.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal("Channel endpoint disposed.", error.Message);
    }

    [Fact]
    public void Emit_AfterEndpointDisposed_FailsFast()
    {
        using var pipe = new ChannelPipe();
        var definition = new EventDefinition<TestPayload>("channel:closed");

        pipe.Left.Dispose();

        var error = Assert.Throws<ChannelClosedException>(() => pipe.Left.Emit(definition, new TestPayload("late")));
        Assert.Equal("Channel endpoint disposed.", error.Message);
    }

    [Fact]
    public async Task Dispose_WhenCalled_ReleasesInboundPumpCancellationSource()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var inboundPumpCancellation = GetInboundPumpCancellationSource(endpoint);

        endpoint.Dispose();

        await AssertEventuallyDisposedAsync(inboundPumpCancellation);
    }

    [Fact]
    public async Task InboundChannelCompletion_WhenObserved_ReleasesInboundPumpCancellationSource()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var inboundPumpCancellation = GetInboundPumpCancellationSource(endpoint);
        var payload = await WaitForClosedAsync(endpoint, () => inbound.Writer.TryComplete());
        Assert.IsType<ChannelClosedException>(payload.Error);
        await AssertEventuallyDisposedAsync(inboundPumpCancellation);
    }

    [Fact]
    public void Emit_WhenOutboundWriterCompleted_ThrowsChannelClosedException()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var definition = new EventDefinition<TestPayload>("channel:writer-completed");

        outbound.Writer.TryComplete();

        var error = Assert.Throws<ChannelClosedException>(() => endpoint.Emit(definition, new TestPayload("late")));
        Assert.Equal("Channel endpoint closed.", error.Message);
    }

    [Fact]
    public async Task Emit_WhenBoundedOutboundFull_ThrowsChannelClosedExceptionWithoutBlocking()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateBounded<ChannelMessage>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
        });
        using var endpoint = new ChannelEndpoint(
            inbound.Reader,
            outbound.Writer,
            new ChannelEndpointOptions { CompleteOutboundOnDispose = true });
        var definition = new EventDefinition<TestPayload>("channel:bounded-full");

        endpoint.Emit(definition, new TestPayload("first"));
        var secondEmit = Task.Run(
            () => Assert.Throws<ChannelClosedException>(() => endpoint.Emit(definition, new TestPayload("second"))),
            TestContext.Current.CancellationToken);

        var error = await secondEmit.WaitAsync(TimeSpan.FromMilliseconds(250), TestContext.Current.CancellationToken);
        Assert.Equal("Channel endpoint closed.", error.Message);
    }

    [Fact]
    public async Task CustomEndpointDispose_WhenConfigured_CompletesOutboundForPairedEndpoint()
    {
        var leftToRight = Channel.CreateUnbounded<ChannelMessage>();
        var rightToLeft = Channel.CreateUnbounded<ChannelMessage>();
        using var left = new ChannelEndpoint(
            rightToLeft.Reader,
            leftToRight.Writer,
            new ChannelEndpointOptions { CompleteOutboundOnDispose = true });
        using var right = new ChannelEndpoint(leftToRight.Reader, rightToLeft.Writer);
        var payload = await WaitForClosedAsync(right, left.Dispose);
        var error = Assert.IsType<ChannelClosedException>(payload.Error);
        Assert.Equal("Channel closed.", error.Message);
    }

    [Fact]
    public async Task InboundNullEnvelope_FaultsEndpointAndEmitsClosedEvent()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var payload = await WaitForClosedAsync(
            endpoint,
            async () => await inbound.Writer.WriteAsync(new ChannelMessage(null), TestContext.Current.CancellationToken));
        Assert.IsType<InvalidOperationException>(payload.Error);
    }

    [Fact]
    public async Task InboundListenerException_FaultsEndpointAndEmitsClosedEvent()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var definition = new EventDefinition<TestPayload>("channel:listener-fault");
        var expected = new InvalidOperationException("listener failed");
        var payload = await TriggerInboundListenerFaultAsync(endpoint, inbound.Writer, definition, expected);
        Assert.Same(expected, payload.Error);
    }

    [Fact]
    public async Task InboundListenerException_CompletesOutboundForPairedEndpointWithOriginalCause()
    {
        using var pipe = new ChannelPipe();
        var definition = new EventDefinition<TestPayload>("channel:paired-listener-fault");
        var expected = new InvalidOperationException("listener failed");
        using var _ = pipe.Right.Subscribe(definition, _ => throw expected);

        var payload = await WaitForClosedAsync(pipe.Left, () => pipe.Left.Emit(definition, new TestPayload("boom")));
        Assert.Same(expected, payload.Error);
    }

    [Fact]
    public async Task InboundListenerException_FaultsPendingInvokeOnPairedEndpointWithOriginalCause()
    {
        using var pipe = new ChannelPipe();
        var pendingDefinition = new InvokeEventDefinition<string, string>("channel:pending-peer-fault");
        var faultingDefinition = new EventDefinition<TestPayload>("channel:pending-peer-listener-fault");
        var expected = new InvalidOperationException("listener failed");
        var client = pipe.Left.CreateInvokeClient(pendingDefinition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        using var _ = pipe.Right.Subscribe(faultingDefinition, _ => throw expected);

        pipe.Left.Emit(faultingDefinition, new TestPayload("boom"));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await pending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task Emit_AfterInboundListenerException_PreservesOriginalTerminalCause()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var faultingDefinition = new EventDefinition<TestPayload>("channel:listener-terminal-cause");
        var lateDefinition = new EventDefinition<TestPayload>("channel:late-after-listener-fault");
        var expected = new InvalidOperationException("listener failed");
        await TriggerInboundListenerFaultAsync(endpoint, inbound.Writer, faultingDefinition, expected);

        var error = Assert.Throws<ChannelClosedException>(() => endpoint.Emit(lateDefinition, new TestPayload("late")));
        Assert.Same(expected, error.InnerException);
    }

    [Fact]
    public async Task Dispose_AfterInboundListenerException_PreservesOriginalTerminalCause()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var faultingDefinition = new EventDefinition<TestPayload>("channel:listener-dispose-terminal-cause");
        var lateDefinition = new EventDefinition<TestPayload>("channel:late-after-dispose");
        var expected = new InvalidOperationException("listener failed");
        await TriggerInboundListenerFaultAsync(endpoint, inbound.Writer, faultingDefinition, expected);

        endpoint.Dispose();

        var error = Assert.Throws<ChannelClosedException>(() => endpoint.Emit(lateDefinition, new TestPayload("late")));
        Assert.Same(expected, error.InnerException);
    }

    [Fact]
    public async Task FaultedInboundChannel_PreservesOriginalExceptionInClosedEvent()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var expected = new InvalidOperationException("channel failed");
        var payload = await WaitForClosedAsync(endpoint, () => inbound.Writer.TryComplete(expected));
        Assert.Same(expected, payload.Error);
    }

    private static async IAsyncEnumerable<int> CountAsync(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var value = 1; value <= count; value++)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
        }
    }

    private static async IAsyncEnumerable<int> Numbers(params int[] values)
    {
        foreach (var value in values)
        {
            await Task.Yield();
            yield return value;
        }
    }

    private static Task<ChannelClosedPayload> WaitForClosedAsync(IEventContext context, Action trigger)
    {
        return WaitForClosedAsync(
            context,
            () =>
            {
                trigger();
                return Task.CompletedTask;
            });
    }

    private static async Task<ChannelClosedPayload> WaitForClosedAsync(IEventContext context, Func<Task> trigger)
    {
        var closed = new TaskCompletionSource<ChannelClosedPayload>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = context.Subscribe(ChannelEvents.Closed, envelope => closed.TrySetResult(envelope.Body));

        await trigger();
        return await closed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    private static async Task<ChannelClosedPayload> TriggerInboundListenerFaultAsync(
        ChannelEndpoint endpoint,
        ChannelWriter<ChannelMessage> inboundWriter,
        EventDefinition<TestPayload> faultingDefinition,
        Exception expected)
    {
        using var _ = endpoint.Subscribe(faultingDefinition, _ => throw expected);

        return await WaitForClosedAsync(
            endpoint,
            async () => await inboundWriter.WriteAsync(
                new ChannelMessage(new EventEnvelope<TestPayload>(faultingDefinition.Id, new TestPayload("boom"))),
                TestContext.Current.CancellationToken));
    }

    private static CancellationTokenSource GetInboundPumpCancellationSource(ChannelEndpoint endpoint)
    {
        var field = typeof(ChannelEndpoint).GetField(
            "_disposeCancellation",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ChannelEndpoint no longer exposes _disposeCancellation.");

        return Assert.IsType<CancellationTokenSource>(field.GetValue(endpoint));
    }

    private static async Task AssertEventuallyDisposedAsync(CancellationTokenSource source)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                _ = source.Token;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), TestContext.Current.CancellationToken);
        }

        Assert.Throws<ObjectDisposedException>(() => _ = source.Token);
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var results = new List<T>();

        await foreach (var value in source.WithCancellation(TestContext.Current.CancellationToken))
        {
            results.Add(value);
        }

        return results;
    }

    private sealed record TestPayload(string Value);
    private sealed record EchoRequest(string Value);
    private sealed record EchoResponse(string Value);
}
