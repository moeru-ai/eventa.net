namespace Eventa;

public static class EventStream
{
    public static IAsyncEnumerable<TResponse> DefineStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return CreateStreamInvoke(
            context,
            eventDefinition,
            cancellationToken,
            invokeId =>
            {
                var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
                context.Emit(sendEvent, new SendPayload<TRequest>(invokeId, request));
                return Task.CompletedTask;
            });
    }

    public static IAsyncEnumerable<TResponse> DefineStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        IAsyncEnumerable<TRequest> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(request);

        return CreateStreamInvoke(
            context,
            eventDefinition,
            cancellationToken,
            async invokeId =>
            {
                var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
                var sendStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.SendStreamEndId);

                try
                {
                    await foreach (var item in request.WithCancellation(cancellationToken).ConfigureAwait(false))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        context.Emit(sendEvent, new SendPayload<TRequest>(invokeId, item));
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    context.Emit(sendStreamEndEvent, new StreamEndPayload(invokeId));
                }
            });
    }

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
        var receiveStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.ReceiveStreamEndId);
        var sync = new object();
        var inflight = new Dictionary<string, CancellationTokenSource>(StringComparer.Ordinal);

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var cancellationSource = new CancellationTokenSource();

            lock (sync)
            {
                inflight[invokeId] = cancellationSource;
            }

            try
            {
                await foreach (var item in handler(request, cancellationSource.Token).ConfigureAwait(false))
                {
                    if (cancellationSource.IsCancellationRequested)
                    {
                        return;
                    }

                    context.Emit(receiveEvent, new ReceivePayload<TResponse>(invokeId, item));
                }

                if (!cancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveStreamEndEvent, new StreamEndPayload(invokeId));
                }
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                if (!cancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveErrorEvent, new ReceiveErrorPayload(invokeId, error));
                }
            }
            finally
            {
                lock (sync)
                {
                    inflight.Remove(invokeId);
                }

                cancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(sendEvent, envelope => _ = HandleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.On(sendAbortEvent, envelope =>
            {
                lock (sync)
                {
                    if (inflight.TryGetValue(envelope.Body.InvokeId, out var cancellationSource))
                    {
                        cancellationSource.Cancel();
                    }
                }
            }),
        };

        return new ActionDisposable(() =>
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }

            lock (sync)
            {
                foreach (var cancellationSource in inflight.Values)
                {
                    cancellationSource.Cancel();
                    cancellationSource.Dispose();
                }

                inflight.Clear();
            }
        });
    }

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
        var sendStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.SendStreamEndId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
        var receiveStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.ReceiveStreamEndId);
        var sync = new object();
        var inflight = new Dictionary<string, RequestStreamInvocationState<TRequest>>(StringComparer.Ordinal);

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            lock (sync)
            {
                if (inflight.TryGetValue(invokeId, out var existing))
                {
                    return existing;
                }

                var created = new RequestStreamInvocationState<TRequest>(invokeId);
                inflight[invokeId] = created;
                created.Execution = HandleInvokeAsync(created);
                return created;
            }
        }

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            try
            {
                await foreach (var item in handler(
                    state.Requests.ReadAll(respectConsumerCancellation: false),
                    state.CancellationSource.Token).ConfigureAwait(false))
                {
                    if (state.CancellationSource.IsCancellationRequested)
                    {
                        return;
                    }

                    context.Emit(receiveEvent, new ReceivePayload<TResponse>(state.InvokeId, item));
                }

                if (!state.CancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveStreamEndEvent, new StreamEndPayload(state.InvokeId));
                }
            }
            catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                if (!state.CancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveErrorEvent, new ReceiveErrorPayload(state.InvokeId, error));
                }
            }
            finally
            {
                lock (sync)
                {
                    inflight.Remove(state.InvokeId);
                }

                state.CancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(sendEvent, envelope =>
            {
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.TryWrite(envelope.Body.Content);
            }),
            context.On(sendStreamEndEvent, envelope =>
            {
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.Complete();
            }),
            context.On(sendAbortEvent, envelope =>
            {
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.Fault(new OperationCanceledException(state.CancellationSource.Token));
                state.CancellationSource.Cancel();
            }),
        };

        return new ActionDisposable(() =>
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }

            lock (sync)
            {
                foreach (var state in inflight.Values)
                {
                    state.Requests.Fault(new OperationCanceledException(state.CancellationSource.Token));
                    state.CancellationSource.Cancel();
                    state.CancellationSource.Dispose();
                }

                inflight.Clear();
            }
        });
    }

    public static Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> ToStreamHandler<TResponse, TRequest>(
        Func<TRequest, Func<TResponse, ValueTask>, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        return (request, cancellationToken) =>
        {
            var responses = new AsyncSignalQueue<TResponse>();

            _ = Task.Run(async () =>
            {
                try
                {
                    await handler(
                        request,
                        response =>
                        {
                            responses.TryWrite(response);
                            return ValueTask.CompletedTask;
                        },
                        cancellationToken).ConfigureAwait(false);
                    responses.Complete();
                }
                catch (Exception error)
                {
                    responses.Fault(error);
                }
            }, CancellationToken.None);

            return responses.ReadAll();
        };
    }

    private static IAsyncEnumerable<TResponse> CreateStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        CancellationToken cancellationToken,
        Func<string, Task> sendRequest)
    {
        var invokeId = IdGenerator.New();
        var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
        var receiveStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.ReceiveStreamEndId);
        var responses = new AsyncSignalQueue<TResponse>();
        var subscriptions = new List<IDisposable>();
        var finished = 0;

        void Cleanup()
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
        }

        void Complete()
        {
            if (Interlocked.Exchange(ref finished, 1) != 0)
            {
                return;
            }

            responses.Complete();
            Cleanup();
        }

        void Fault(Exception error)
        {
            if (Interlocked.Exchange(ref finished, 1) != 0)
            {
                return;
            }

            responses.Fault(error);
            Cleanup();
        }

        void AbortWithCompletion(Exception? error)
        {
            if (Interlocked.Exchange(ref finished, 1) != 0)
            {
                return;
            }

            context.Emit(sendAbortEvent, new AbortPayload(invokeId));

            if (error is null)
            {
                responses.Complete();
            }
            else
            {
                responses.Fault(error);
            }

            Cleanup();
        }

        subscriptions.Add(context.On(receiveEvent, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, invokeId))
            {
                return;
            }

            responses.TryWrite(envelope.Body.Content);
        }));

        subscriptions.Add(context.On(receiveErrorEvent, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, invokeId))
            {
                return;
            }

            Fault(envelope.Body.Error);
        }));

        subscriptions.Add(context.On(receiveStreamEndEvent, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, invokeId))
            {
                return;
            }

            Complete();
        }));

        if (cancellationToken.CanBeCanceled)
        {
            var registration = cancellationToken.Register(
                () => AbortWithCompletion(new OperationCanceledException(cancellationToken)));
            subscriptions.Add(new ActionDisposable(registration.Dispose));

            if (cancellationToken.IsCancellationRequested)
            {
                AbortWithCompletion(new OperationCanceledException(cancellationToken));
                return responses.ReadAll(onDispose: () => ValueTask.CompletedTask);
            }
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await sendRequest(invokeId).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                Fault(error);
            }
        }, CancellationToken.None);

        return responses.ReadAll(onDispose: () =>
        {
            AbortWithCompletion(null);
            return ValueTask.CompletedTask;
        });
    }

    private sealed class RequestStreamInvocationState<TRequest>(string invokeId)
    {
        public string InvokeId { get; } = invokeId;

        public AsyncSignalQueue<TRequest> Requests { get; } = new();

        public CancellationTokenSource CancellationSource { get; } = new();

        public Task? Execution { get; set; }
    }
}
