namespace Eventa.Tests;

public class InvokeTests
{
    [Fact]
    public async Task DefineInvoke_HandlesRequestResponse()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<UserResponse, UserRequest>("user-lookup");

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) => Task.FromResult(new UserResponse($"{request.Name}-{request.Age}")));

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var result = await invoke(new UserRequest("alice", 25), CancellationToken.None);

        Assert.Equal(new UserResponse("alice-25"), result);
    }

    [Fact]
    public async Task DefineInvoke_SupportsSyncLazyContextFactory()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<UserResponse, UserRequest>("user-lookup");
        var factoryCalls = 0;

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) => Task.FromResult(new UserResponse($"{request.Name}-{request.Age}")));

        var invoke = EventInvoke.DefineInvoke(() =>
        {
            factoryCalls++;
            return context;
        }, definition);

        var result = await invoke(new UserRequest("alice", 25), CancellationToken.None);

        Assert.Equal(1, factoryCalls);
        Assert.Equal(new UserResponse("alice-25"), result);
    }

    [Fact]
    public async Task DefineInvoke_PropagatesHandlerErrorsWithRequestData()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<UserResponse, UserRequest>("user-lookup");

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) => Task.FromException<UserResponse>(
                new InvalidOperationException(
                    $"Error processing request for {request.Name} aged {request.Age}")));

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => invoke(new UserRequest("alice", 25), CancellationToken.None));

        Assert.Equal("Error processing request for alice aged 25", actual.Message);
    }

    [Fact]
    public async Task DefineInvoke_PreservesTheExactHandlerErrorInstance()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("user-lookup");
        var expected = new InvalidOperationException("invoke handler failed");

        using var _ = EventInvoke.DefineInvokeHandler<string, string>(
            context,
            definition,
            (string _, CancellationToken _) => Task.FromException<string>(expected));

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => invoke("request", CancellationToken.None));

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task DefineInvoke_AbortsInvokeAndNotifiesHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, CancelRequest>("cancellable");
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            async (CancelRequest _, CancellationToken cancellationToken) =>
            {
                using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return "completed";
            });

        var invoke = EventInvoke.DefineInvoke(context, definition);
        using var cancellationSource = new CancellationTokenSource();

        var pending = invoke(new CancelRequest(1), cancellationSource.Token);
        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DefineInvoke_IsolatesConcurrentRequests()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("double");

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) => Task.FromResult(request * 2));

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var results = await Task.WhenAll(
            invoke(10, CancellationToken.None),
            invoke(20, CancellationToken.None),
            invoke(50, CancellationToken.None));

        Assert.Equal([20, 40, 100], results);
    }

    [Fact]
    public async Task DefineInvokeHandler_DeduplicatesTheSameHandlerInstance()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("double");
        var callCount = 0;
        Func<int, CancellationToken, Task<int>> handler = (request, _) =>
        {
            callCount++;
            return Task.FromResult(request * 2);
        };

        using var _ = EventInvoke.DefineInvokeHandler(context, definition, handler);
        using var __ = EventInvoke.DefineInvokeHandler(context, definition, handler);

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var result = await invoke(21, CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task DefineInvokeHandler_ReturnedSubscriptionRemovesOnlyTheRequestedHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("echo");
        var strongCalls = 0;
        var weakCalls = 0;

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) =>
            {
                strongCalls++;
                return Task.FromResult(request);
            });

        var weakSubscription = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            (request, _) =>
            {
                weakCalls++;
                return Task.FromResult(request);
            });

        var invoke = EventInvoke.DefineInvoke(context, definition);

        await invoke("test", CancellationToken.None);
        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        weakSubscription.Dispose();

        await invoke("test", CancellationToken.None);
        Assert.Equal(2, strongCalls);
        Assert.Equal(1, weakCalls);
    }

    [Fact]
    public async Task DefineInvokeHandler_AcceptsRequestStreamProtocolMessages()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum");
        var invokeId = "invoke-1";
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendStreamEndEvent = new EventDefinition<StreamEndPayload>(definition.SendStreamEndId);
        var receiveEvent = new EventDefinition<ReceivePayload<int>>(definition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(definition.ReceiveErrorId);
        var response = new TaskCompletionSource<ReceivePayload<int>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<int>();

        using var _ = context.On(receiveEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                response.TrySetResult(envelope.Body);
            }
        });
        using var __ = context.On(receiveErrorEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                response.TrySetException(envelope.Body.Error);
            }
        });
        using var ___ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            async (request, cancellationToken) =>
            {
                var sum = 0;
                await foreach (var value in request.WithCancellation(cancellationToken))
                {
                    received.Add(value);
                    sum += value;
                }

                return sum;
            });

        context.Emit(sendEvent, new SendPayload<int>(invokeId, 1));
        context.Emit(sendEvent, new SendPayload<int>(invokeId, 2));
        context.Emit(sendEvent, new SendPayload<int>(invokeId, 3));
        context.Emit(sendStreamEndEvent, new StreamEndPayload(invokeId));

        var result = await response.Task;

        Assert.Equal([1, 2, 3], received);
        Assert.Equal(new ReceivePayload<int>(invokeId, 6), result);
    }

    [Fact]
    public async Task DefineInvokeHandler_NotifiesHandlerWhenRequestStreamIsAborted()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-abort");
        var invokeId = "invoke-1";
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<int>();
        Exception? handlerError = null;

        using var _ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            async (IAsyncEnumerable<int> request, CancellationToken cancellationToken) =>
            {
                using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));
                var sum = 0;

                try
                {
                    await foreach (var value in request.WithCancellation(cancellationToken))
                    {
                        received.Add(value);
                        sum += value;
                    }
                }
                catch (Exception error)
                {
                    handlerError = error;
                }
                finally
                {
                    handlerCompleted.TrySetResult(true);
                }

                return sum;
            });

        context.Emit(sendEvent, new SendPayload<int>(invokeId, 1));
        context.Emit(sendEvent, new SendPayload<int>(invokeId, 2));
        context.Emit(sendEvent, new SendPayload<int>(invokeId, 3));
        context.Emit(sendEvent, new SendPayload<int>(invokeId, 4));
        context.Emit(sendAbortEvent, new AbortPayload(invokeId, "stop"));

        await handlerCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal([1, 2, 3, 4], received);
        Assert.NotNull(handlerError);
        Assert.IsAssignableFrom<OperationCanceledException>(handlerError);
    }

    private sealed record CancelRequest(int Value);

    private sealed record UserRequest(string Name, int Age);

    private sealed record UserResponse(string Id);
}
