using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

using Eventa.Adapters.Channels;

using ChannelClosedException = Eventa.Adapters.Channels.ChannelClosedException;
using SystemChannelClosedException = System.Threading.Channels.ChannelClosedException;

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
    public async Task Dispose_FaultsActiveStreamBeforeClosedEvent()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<int, int>("channel:dispose-stream");
        var closedObserved = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = pipe.Right.RegisterStreamHandler(definition, PendingAsync);

        var client = pipe.Left.CreateInvokeStreamClient(definition);
        await using var enumerator = client.InvokeAsync(1, CancellationToken.None)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        var pendingMoveNext = enumerator.MoveNextAsync().AsTask();

        using var __ = pipe.Left.Subscribe(ChannelEvents.Closed, _ =>
        {
            if (pendingMoveNext.IsFaulted)
            {
                closedObserved.TrySetResult(true);
            }
            else
            {
                closedObserved.TrySetException(new InvalidOperationException("Closed event ran before stream faulted."));
            }
        });

        pipe.Left.Dispose();

        var error = await Assert.ThrowsAsync<ChannelClosedException>(
            async () => await pendingMoveNext.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
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
    public void ListenerOperations_AfterEndpointDisposed_FailFast()
    {
        using var pipe = new ChannelPipe();
        var definition = new EventDefinition<TestPayload>("channel:post-terminal-listeners");
        var matchExpression = new MatchExpression<TestPayload>("channel:post-terminal-match", _ => true);
        Action<EventEnvelope<TestPayload>> handler = _ => { };

        pipe.Left.Dispose();

        AssertDisposed(() => pipe.Left.Subscribe(definition, handler));
        AssertDisposed(() => pipe.Left.SubscribeOnce(definition, handler));
        AssertDisposed(() => pipe.Left.Unsubscribe(definition, handler));
        AssertDisposed(() => pipe.Left.Subscribe(matchExpression, handler));
        AssertDisposed(() => pipe.Left.SubscribeOnce(matchExpression, handler));
        AssertDisposed(() => pipe.Left.Unsubscribe(matchExpression, handler));
    }

    [Fact]
    public void HandlerRegistration_AfterEndpointDisposed_FailsFast()
    {
        using var pipe = new ChannelPipe();
        var invokeDefinition = new InvokeEventDefinition<string, string>("channel:post-terminal-invoke-handler");
        var streamDefinition = new InvokeEventDefinition<int, int>("channel:post-terminal-stream-handler");

        pipe.Left.Dispose();

        AssertDisposed(() => pipe.Left.RegisterInvokeHandler(
            invokeDefinition,
            static (request, _) => Task.FromResult(request)));
        AssertDisposed(() => pipe.Left.RegisterStreamHandler(streamDefinition, CountAsync));
    }

    [Fact]
    public void CreateInvokeClient_AfterEndpointDisposed_FirstInvokeFailsFast()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<string, string>("channel:post-terminal-client");

        pipe.Left.Dispose();

        var client = pipe.Left.CreateInvokeClient(definition);
        Assert.NotNull(client);

        AssertDisposed(() => client.InvokeAsync("late", TestContext.Current.CancellationToken));
    }

    [Fact]
    public void CreateInvokeStreamClient_AfterEndpointDisposed_FirstInvokeFailsFast()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<int, int>("channel:post-terminal-stream-client");

        pipe.Left.Dispose();

        var client = pipe.Left.CreateInvokeStreamClient(definition);
        Assert.NotNull(client);

        AssertDisposed(() => client.InvokeAsync(1, TestContext.Current.CancellationToken));
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
    public void Emit_WhenOutboundWriterCompleted_PropagatesWriterFailureUnchanged()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var definition = new EventDefinition<TestPayload>("channel:writer-completed");

        outbound.Writer.TryComplete();

        var error = Assert.Throws<SystemChannelClosedException>(() => endpoint.Emit(definition, new TestPayload("late")));
        Assert.Null(error.InnerException);
    }

    [Fact]
    public void Emit_WhenOutboundWriterFaulted_PropagatesOriginalWriterFailure()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var definition = new EventDefinition<TestPayload>("channel:writer-faulted");
        var expected = new InvalidOperationException("writer failed");

        outbound.Writer.TryComplete(expected);

        var error = Assert.Throws<SystemChannelClosedException>(() => endpoint.Emit(definition, new TestPayload("late")));
        Assert.Same(expected, error.InnerException);
    }

    [Fact]
    public async Task InvokeAsync_WhenOutboundWriterFaulted_PropagatesOriginalWriterFailure()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var definition = new InvokeEventDefinition<string, string>("channel:invoke-writer-faulted");
        var expected = new InvalidOperationException("writer failed");
        var client = endpoint.CreateInvokeClient(definition);

        outbound.Writer.TryComplete(expected);

        var error = await Assert.ThrowsAsync<SystemChannelClosedException>(
            async () => await client.InvokeAsync("request", TestContext.Current.CancellationToken));
        Assert.Same(expected, error.InnerException);
    }

    [Fact]
    public async Task Emit_WhenBoundedOutboundFull_FailsFastWithoutBlocking()
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
            () => Assert.Throws<InvalidOperationException>(() => endpoint.Emit(definition, new TestPayload("second"))),
            TestContext.Current.CancellationToken);

        var error = await secondEmit.WaitAsync(TimeSpan.FromMilliseconds(250), TestContext.Current.CancellationToken);
        Assert.Equal("Outbound channel could not accept the message immediately.", error.Message);
    }

    [Fact]
    public async Task Emit_WhenWriterSignalsWritableButRejectsTryWrite_FailsFastWithoutCallingWriteAsync()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = new ProbeRejectingWriter();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound);
        var definition = new EventDefinition<TestPayload>("channel:writer-contention");

        var emitTask = Task.Run(
            () => Assert.Throws<InvalidOperationException>(() => endpoint.Emit(definition, new TestPayload("late"))),
            TestContext.Current.CancellationToken);

        var error = await emitTask.WaitAsync(TimeSpan.FromMilliseconds(250), TestContext.Current.CancellationToken);
        Assert.Equal("Outbound channel could not accept the message immediately.", error.Message);
        Assert.Equal(0, Volatile.Read(ref outbound.WriteAsyncCalls));
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
    public async Task Dispose_WhenPipeAndEndpointDisposalsRace_EmitsClosedEventOncePerEndpoint()
    {
        using var pipe = new ChannelPipe();
        IEventContext leftContext = pipe.Left;
        IEventContext rightContext = pipe.Right;
        var leftClosed = CreateSingleClosedObserver(pipe.Left, "left");
        var rightClosed = CreateSingleClosedObserver(pipe.Right, "right");

        await Task.WhenAll(
            Task.Run(pipe.Dispose, TestContext.Current.CancellationToken),
            Task.Run(leftContext.Dispose, TestContext.Current.CancellationToken),
            Task.Run(rightContext.Dispose, TestContext.Current.CancellationToken));

        await leftClosed.Observed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await rightClosed.Observed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(1, Volatile.Read(ref leftClosed.Count));
        Assert.Equal(1, Volatile.Read(ref rightClosed.Count));
    }

    [Fact]
    public void CustomEndpointDispose_WhenNotConfigured_LeavesPairedEndpointUsable()
    {
        var leftToRight = Channel.CreateUnbounded<ChannelMessage>();
        var rightToLeft = Channel.CreateUnbounded<ChannelMessage>();
        using var left = new ChannelEndpoint(
            rightToLeft.Reader,
            leftToRight.Writer,
            new ChannelEndpointOptions { CompleteOutboundOnDispose = false });
        using var right = new ChannelEndpoint(leftToRight.Reader, rightToLeft.Writer);
        var definition = new EventDefinition<TestPayload>("channel:paired-still-open");

        left.Dispose();

        var error = Record.Exception(() => right.Emit(definition, new TestPayload("still-open")));
        Assert.Null(error);
    }

    [Fact]
    public async Task InboundCompletion_WhenOutboundNotOwned_LeavesOutboundWriterUsable()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(
            inbound.Reader,
            outbound.Writer,
            new ChannelEndpointOptions { CompleteOutboundOnDispose = false });
        var payload = await WaitForClosedAsync(endpoint, () => inbound.Writer.TryComplete());
        var error = Assert.IsType<ChannelClosedException>(payload.Error);

        Assert.Equal("Channel closed.", error.Message);
        Assert.True(outbound.Writer.TryWrite(
            new ChannelMessage(new EventEnvelope<TestPayload>("channel:external-owner", new TestPayload("still-open")))));
    }

    [Fact]
    public async Task InboundFault_WhenOutboundNotOwned_LeavesOutboundWriterUsable()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(
            inbound.Reader,
            outbound.Writer,
            new ChannelEndpointOptions { CompleteOutboundOnDispose = false });
        var expected = new InvalidOperationException("inbound failed");
        var payload = await WaitForClosedAsync(endpoint, () => inbound.Writer.TryComplete(expected));

        Assert.Same(expected, payload.Error);
        Assert.True(outbound.Writer.TryWrite(
            new ChannelMessage(new EventEnvelope<TestPayload>("channel:external-owner-fault", new TestPayload("still-open")))));
    }

    [Fact]
    public async Task InboundNullEnvelope_FaultsEndpointAndEmitsClosedEvent()
    {
        var inbound = Channel.CreateUnbounded<ChannelMessage>();
        var outbound = Channel.CreateUnbounded<ChannelMessage>();
        using var endpoint = new ChannelEndpoint(inbound.Reader, outbound.Writer);
        var payload = await WaitForClosedAsync(
            endpoint,
            async () =>
                // Intentionally violate the public non-null contract to verify that malformed
                // transport input still faults the endpoint deterministically.
                await inbound.Writer.WriteAsync(new ChannelMessage(null!), TestContext.Current.CancellationToken));
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
    public async Task InboundListenerException_FaultsActiveStreamOnPairedEndpointWithOriginalCause()
    {
        using var pipe = new ChannelPipe();
        var streamDefinition = new InvokeEventDefinition<int, int>("channel:stream-peer-fault");
        var faultingDefinition = new EventDefinition<TestPayload>("channel:stream-peer-listener-fault");
        var expected = new InvalidOperationException("listener failed");

        using var _ = pipe.Right.RegisterStreamHandler(streamDefinition, PendingAsync);
        await using var enumerator = pipe.Left
            .CreateInvokeStreamClient(streamDefinition)
            .InvokeAsync(1, CancellationToken.None)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        var pendingMoveNext = enumerator.MoveNextAsync().AsTask();

        using var __ = pipe.Left.Subscribe(faultingDefinition, _ => throw expected);

        pipe.Right.Emit(faultingDefinition, new TestPayload("boom"));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await pendingMoveNext.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task Dispose_WhenClosedEventListenerThrows_StillFaultsPendingInvoke()
    {
        using var pipe = new ChannelPipe();
        var definition = new InvokeEventDefinition<string, string>("channel:dispose-closed-listener-fault");
        var client = pipe.Left.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        using var _ = pipe.Left.Subscribe(ChannelEvents.Closed, _ => throw new InvalidOperationException("closed listener failed"));

        pipe.Left.Dispose();

        var error = await Assert.ThrowsAsync<ChannelClosedException>(
            async () => await pending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal("Channel endpoint disposed.", error.Message);
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

    private static async IAsyncEnumerable<int> PendingAsync(
        int _,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        yield break;
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

    private static void AssertDisposed(Action action)
    {
        var error = Assert.Throws<ChannelClosedException>(action);
        Assert.Equal("Channel endpoint disposed.", error.Message);
    }

    private static ClosedObserver CreateSingleClosedObserver(IEventContext context, string side)
    {
        var observer = new ClosedObserver(
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
        observer.Subscription = context.Subscribe(ChannelEvents.Closed, _ =>
        {
            if (Interlocked.Increment(ref observer.Count) == 1)
            {
                observer.Observed.TrySetResult(true);
                return;
            }

            observer.Observed.TrySetException(new InvalidOperationException($"{side} closed event observed more than once."));
        });

        return observer;
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

    private sealed class ClosedObserver(TaskCompletionSource<bool> observed)
    {
        public IDisposable Subscription { get; set; } = null!;

        public TaskCompletionSource<bool> Observed { get; } = observed;

        public int Count;
    }

    private sealed class ProbeRejectingWriter : ChannelWriter<ChannelMessage>
    {
        public int WriteAsyncCalls;

        public override bool TryComplete(Exception? error = null)
        {
            return true;
        }

        public override bool TryWrite(ChannelMessage item)
        {
            return false;
        }

        public override ValueTask<bool> WaitToWriteAsync(CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(true);
        }

        public override ValueTask WriteAsync(ChannelMessage item, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref WriteAsyncCalls);
            return ValueTask.FromException(new InvalidOperationException("WriteAsync should not be called."));
        }
    }

    private sealed record TestPayload(string Value);
    private sealed record EchoRequest(string Value);
    private sealed record EchoResponse(string Value);
}
