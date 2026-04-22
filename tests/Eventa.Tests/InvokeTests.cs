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
    public async Task DefineInvoke_WithPreCanceledToken_EmitsAbortOnlyOnce()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pre-canceled");
        var sendEvent = new EventDefinition<SendPayload<string>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var sendCount = 0;
        var abortCount = 0;

        using var _ = context.On(sendEvent, _ => sendCount++);
        using var __ = context.On(sendAbortEvent, _ => abortCount++);

        var invoke = EventInvoke.DefineInvoke(context, definition);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await invoke("request", cancellationSource.Token));

        Assert.Equal(0, sendCount);
        Assert.Equal(1, abortCount);
    }

    [Fact]
    public async Task DefineInvoke_FatalEventBeforeEmit_DoesNotSendRequest()
    {
        var fatalError = new InvalidOperationException("fatal-before-send");
        var fatalEvent = new EventDefinition<Exception>("fatal-before-send-event");
        var definition = new InvokeEventDefinition<string, string>("fatal-before-send");
        var sendEvent = new EventDefinition<SendPayload<string>>(definition.SendEventId);
        var context = new FatalEventDuringSubscriptionContext(fatalEvent.Id, fatalError);
        var sendCount = 0;

        context.RegisterAbortEvent(fatalEvent);

        using var _ = context.On(sendEvent, _ => sendCount++);

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await invoke("request", CancellationToken.None));

        Assert.Same(fatalError, error);
        Assert.Equal(0, sendCount);
    }

    [Fact]
    public async Task DefineInvokeHandler_DoesNotEmitResponseAfterAbortWinsTheRace()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("unary-abort-wins");
        var invokeId = "invoke-unary-abort-wins";
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<int>>(definition.ReceiveEventId);
        var handlerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var responseEmitted = new TaskCompletionSource<ReceivePayload<int>>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = context.On(receiveEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                responseEmitted.TrySetResult(envelope.Body);
            }
        });
        using var __ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            async (int _, CancellationToken cancellationToken) =>
            {
                handlerStarted.TrySetResult(true);
                await allowCompletion.Task.WaitAsync(TestContext.Current.CancellationToken);
                return 42;
            });

        context.Emit(sendEvent, new SendPayload<int>(invokeId, 1));
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        context.Emit(sendAbortEvent, new AbortPayload(invokeId, "stop"));
        allowCompletion.TrySetResult();
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(responseEmitted.Task.IsCompleted);
    }

    [Fact]
    public async Task DefineInvoke_DoesNotEmitAbortAfterResponseWinsTheRace()
    {
        var definition = new InvokeEventDefinition<string, string>("cancel-after-response");
        var sendEvent = new EventDefinition<SendPayload<string>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<string>>(definition.ReceiveEventId);
        var context = new BlockingDisposeEventContext(receiveEvent.Id);
        var abortCount = 0;

        using var _ = context.On(sendAbortEvent, _ => abortCount++);
        using var __ = context.On(sendEvent, envelope =>
        {
            Task.Run(() => context.Emit(
                receiveEvent,
                new ReceivePayload<string>(envelope.Body.InvokeId, "completed")));
        });

        var invoke = EventInvoke.DefineInvoke(context, definition);
        using var cancellationSource = new CancellationTokenSource();

        var pending = invoke("request", cancellationSource.Token);
        await context.BlockedDisposeStarted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        cancellationSource.Cancel();
        context.ReleaseBlockedDispose();

        var result = await pending;

        Assert.Equal("completed", result);
        Assert.Equal(0, abortCount);
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
    public async Task DefineInvokeHandler_AcceptsEmptyRequestStreamProtocolMessages()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-empty");
        var invokeId = "invoke-empty";
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

        context.Emit(sendStreamEndEvent, new StreamEndPayload(invokeId));

        var result = await response.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Empty(received);
        Assert.Equal(new ReceivePayload<int>(invokeId, 0), result);
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

    [Fact]
    public async Task DefineInvokeHandler_NotifiesHandlerWhenRequestStreamIsAbortedBeforeFirstItem()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-abort-before-first-item");
        var invokeId = "invoke-1";
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

        context.Emit(sendAbortEvent, new AbortPayload(invokeId, "stop"));

        await handlerCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Empty(received);
        Assert.NotNull(handlerError);
        Assert.IsAssignableFrom<OperationCanceledException>(handlerError);
    }

    [Fact]
    public async Task DefineInvokeHandler_RequestStreamAbortDoesNotEmitResponseAfterAbortWinsTheRace()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-abort-ignores-cancel");
        var invokeId = "invoke-request-abort-wins";
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<int>>(definition.ReceiveEventId);
        var handlerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var responseEmitted = new TaskCompletionSource<ReceivePayload<int>>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var _ = context.On(receiveEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                responseEmitted.TrySetResult(envelope.Body);
            }
        });
        using var __ = EventInvoke.DefineInvokeHandler(
            context,
            definition,
            async (IAsyncEnumerable<int> request, CancellationToken cancellationToken) =>
            {
                handlerStarted.TrySetResult(true);

                try
                {
                    await foreach (var _ in request.WithCancellation(cancellationToken))
                    {
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }

                await allowCompletion.Task.WaitAsync(TestContext.Current.CancellationToken);
                return 42;
            });

        context.Emit(sendEvent, new SendPayload<int>(invokeId, 1));
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        context.Emit(sendAbortEvent, new AbortPayload(invokeId, "stop"));
        allowCompletion.TrySetResult();
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(responseEmitted.Task.IsCompleted);
    }

    private sealed record CancelRequest(int Value);

    private sealed record UserRequest(string Name, int Age);

    private sealed record UserResponse(string Id);

    private sealed class FatalEventDuringSubscriptionContext(string fatalEventId, Exception fatalError) : IEventContext
    {
        private readonly EventContext _inner = new();

        public IDictionary<string, object> Extensions => _inner.Extensions;

        public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
        {
            _inner.Emit(eventDefinition, payload);
        }

        public void Emit<TPayload, TOptions>(
            EventDefinition<TPayload> eventDefinition,
            TPayload payload,
            TOptions options)
            where TOptions : class
        {
            _inner.Emit(eventDefinition, payload, options);
        }

        public IDisposable On<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>> handler)
        {
            var subscription = _inner.On(eventDefinition, handler);

            if (StringComparer.Ordinal.Equals(eventDefinition.Id, fatalEventId))
            {
                handler(new EventEnvelope<TPayload>(eventDefinition.Id, (TPayload)(object)fatalError));
            }

            return subscription;
        }

        public IDisposable Once<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>> handler)
        {
            return _inner.Once(eventDefinition, handler);
        }

        public void Off<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>>? handler = null)
        {
            _inner.Off(eventDefinition, handler);
        }

        public IDisposable On<TPayload>(
            MatchExpression<TPayload> matchExpression,
            Action<EventEnvelope<TPayload>> handler)
        {
            return _inner.On(matchExpression, handler);
        }

        public void Dispose()
        {
            _inner.Dispose();
        }
    }

    private sealed class BlockingDisposeEventContext(string blockedEventId) : IEventContext
    {
        private readonly Lock _sync = new();
        private readonly Dictionary<string, HashSet<Delegate>> _listeners = new(StringComparer.Ordinal);
        private readonly TaskCompletionSource<bool> _blockedDisposeStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _releaseBlockedDispose =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IDictionary<string, object> Extensions { get; } = new Dictionary<string, object>();

        public Task BlockedDisposeStarted => _blockedDisposeStarted.Task;

        public void ReleaseBlockedDispose()
        {
            _releaseBlockedDispose.TrySetResult(true);
        }

        public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
        {
            ArgumentNullException.ThrowIfNull(eventDefinition);

            List<Action<EventEnvelope<TPayload>>> listeners;
            lock (_sync)
            {
                listeners = _listeners.TryGetValue(eventDefinition.Id, out var registeredListeners)
                    ? [.. registeredListeners.Cast<Action<EventEnvelope<TPayload>>>()]
                    : [];
            }

            var envelope = new EventEnvelope<TPayload>(eventDefinition.Id, payload);
            foreach (var listener in listeners)
            {
                listener(envelope);
            }
        }

        public void Emit<TPayload, TOptions>(
            EventDefinition<TPayload> eventDefinition,
            TPayload payload,
            TOptions _)
            where TOptions : class
        {
            Emit(eventDefinition, payload);
        }

        public IDisposable On<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>> handler)
        {
            ArgumentNullException.ThrowIfNull(eventDefinition);
            ArgumentNullException.ThrowIfNull(handler);

            lock (_sync)
            {
                if (!_listeners.TryGetValue(eventDefinition.Id, out var listeners))
                {
                    listeners = [];
                    _listeners[eventDefinition.Id] = listeners;
                }

                listeners.Add(handler);
            }

            return new ActionDisposable(() =>
            {
                if (StringComparer.Ordinal.Equals(eventDefinition.Id, blockedEventId))
                {
                    _blockedDisposeStarted.TrySetResult(true);
                    _releaseBlockedDispose.Task.GetAwaiter().GetResult();
                }

                lock (_sync)
                {
                    if (_listeners.TryGetValue(eventDefinition.Id, out var listeners))
                    {
                        listeners.Remove(handler);
                        if (listeners.Count == 0)
                        {
                            _listeners.Remove(eventDefinition.Id);
                        }
                    }
                }
            });
        }

        public IDisposable Once<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>> handler)
        {
            throw new NotSupportedException();
        }

        public void Off<TPayload>(
            EventDefinition<TPayload> eventDefinition,
            Action<EventEnvelope<TPayload>>? handler = null)
        {
            throw new NotSupportedException();
        }

        public IDisposable On<TPayload>(
            MatchExpression<TPayload> matchExpression,
            Action<EventEnvelope<TPayload>> handler)
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            lock (_sync)
            {
                _listeners.Clear();
            }
        }
    }
}
