namespace Eventa;

public static class EventStream
{
    // Client invoke API

    public static IAsyncEnumerable<TResponse> DefineStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        var bindings = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);

        return CreateStreamInvoke(
            context,
            bindings,
            cancellationToken,
            sendDispatchMode: SendDispatchMode.InlineAfterCancellationArmed,
            (invokeId, requestCancellationToken) =>
            {
                if (requestCancellationToken.IsCancellationRequested)
                {
                    return Task.CompletedTask;
                }

                context.Emit(bindings.Send, new SendPayload<TRequest>(invokeId, request));
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

        var bindings = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);

        return CreateStreamInvoke(
            context,
            bindings,
            cancellationToken,
            sendDispatchMode: SendDispatchMode.QueueOnThreadPool,
            async (invokeId, requestCancellationToken) =>
            {
                if (requestCancellationToken.IsCancellationRequested) return;

                try
                {
                    await foreach (var item in request.WithCancellation(requestCancellationToken).ConfigureAwait(false))
                    {
                        if (requestCancellationToken.IsCancellationRequested) return;

                        context.Emit(bindings.Send, new SendPayload<TRequest>(invokeId, item));
                    }
                }
                catch (OperationCanceledException) when (requestCancellationToken.IsCancellationRequested) { return; }

                if (requestCancellationToken.IsCancellationRequested) return;

                context.Emit(bindings.SendStreamEnd, new StreamEndPayload(invokeId));
            });
    }

    // Handler registration API

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new InvocationCancellationTracker();

        async Task<bool> TryForwardResponsesAsync(
            string invokeId,
            CancellationTokenSource cancellationSource,
            IAsyncEnumerable<TResponse> responses)
        {
            // Avoid starting handler-stream enumeration after an early abort won.
            if (cancellationSource.IsCancellationRequested) return false;

            await foreach (var item in responses.ConfigureAwait(false))
            {
                // Cancellation can win after MoveNextAsync but before we emit this item.
                if (cancellationSource.IsCancellationRequested) return false;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(invokeId, item));
            }

            // Do not send stream-end if cancellation arrived after the last item.
            if (cancellationSource.IsCancellationRequested) return false;

            context.Emit(events.ReceiveStreamEnd, new StreamEndPayload(invokeId));
            return true;
        }

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var cancellationSource = inflight.BeginTracking(invokeId);

            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (cancellationSource.IsCancellationRequested) return;

                var responses = handler(request, cancellationSource.Token);
                var forwarded = await TryForwardResponsesAsync(invokeId, cancellationSource, responses);
                if (!forwarded) return;
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (cancellationSource.IsCancellationRequested) return;

                context.Emit(events.ReceiveError, new ReceiveErrorPayload(invokeId, error));
            }
            finally
            {
                inflight.StopTracking(invokeId);
                cancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(events.Send, envelope => _ = HandleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.On(events.SendAbort, envelope => inflight.TryCancel(envelope.Body.InvokeId)),
        };

        return new HandlerRegistration(subscriptions, inflight.CancelAllAndDispose);
    }

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new RequestStreamInvocationTracker<TRequest>();

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            return inflight.GetOrCreate(invokeId, HandleInvokeAsync);
        }

        async Task<bool> TryForwardResponsesAsync(
            RequestStreamInvocationState<TRequest> state,
            IAsyncEnumerable<TResponse> responses)
        {
            // Avoid starting handler-stream enumeration after an early abort won.
            if (state.CancellationSource.IsCancellationRequested) return false;

            await foreach (var item in responses.ConfigureAwait(false))
            {
                // Cancellation can win after MoveNextAsync but before we emit this item.
                if (state.CancellationSource.IsCancellationRequested) return false;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(state.InvokeId, item));
            }

            // Do not send stream-end if cancellation arrived after the last item.
            if (state.CancellationSource.IsCancellationRequested) return false;

            context.Emit(events.ReceiveStreamEnd, new StreamEndPayload(state.InvokeId));
            return true;
        }

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (state.CancellationSource.IsCancellationRequested) return;

                var responses = handler(state.Requests.ReadAll(respectConsumerCancellation: false), state.CancellationSource.Token);
                var forwarded = await TryForwardResponsesAsync(state, responses);
                if (!forwarded) return;
            }
            catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (state.CancellationSource.IsCancellationRequested) return;

                context.Emit(events.ReceiveError, new ReceiveErrorPayload(state.InvokeId, error));
            }
            finally
            {
                inflight.Remove(state);
                state.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(events.Send, envelope =>
            {
                GetOrCreateState(envelope.Body.InvokeId).Requests.TryWrite(envelope.Body.Content);
            }),
            context.On(events.SendStreamEnd, envelope =>
            {
                // Keep empty request streams as a supported C# contract even though
                // current TypeScript stream.ts ignores unknown invokeIds here.
                GetOrCreateState(envelope.Body.InvokeId).Requests.Complete();
            }),
            context.On(events.SendAbort, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract; current
                // TypeScript stream.ts also materializes unknown invokeIds on abort
                // so the handler still observes cancellation.
                GetOrCreateState(envelope.Body.InvokeId).Abort();
            }),
        };

        return new HandlerRegistration(subscriptions, inflight.AbortAllAndDispose);
    }

    // Handler adapters

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

    // Client operation

    private static IAsyncEnumerable<TResponse> CreateStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        CancellationToken cancellationToken,
        SendDispatchMode sendDispatchMode,
        Func<string, CancellationToken, Task> sendRequest)
    {
        return new PendingStreamInvokeOperation<TResponse, TRequest>(
            context,
            events,
            cancellationToken,
            sendDispatchMode,
            sendRequest).Run();
    }

    private sealed class PendingStreamInvokeOperation<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        CancellationToken cancellationToken,
        SendDispatchMode sendDispatchMode,
        Func<string, CancellationToken, Task> sendRequest)
    {
        private readonly string _invokeId = IdGenerator.New();
        private readonly AsyncSignalQueue<TResponse> _responses = new();
        private readonly CancellationTokenSource _requestCancellationSource = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : new CancellationTokenSource();
        private readonly List<IDisposable> _subscriptions = [];
        private int _finished;

        public IAsyncEnumerable<TResponse> Run()
        {
            SubscribeToResponses();

            if (!ClientCancellation.TryArm(_subscriptions, AbortFromClient, cancellationToken))
            {
                return CreateResultStream(NoopOnDisposeAsync);
            }

            DispatchSendRequest();

            return CreateResultStream(AbortOnDisposeAsync);
        }

        private void SubscribeToResponses()
        {
            _subscriptions.Add(context.On(events.Receive, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                _responses.TryWrite(envelope.Body.Content);
            }));

            _subscriptions.Add(context.On(events.ReceiveError, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                Fault(envelope.Body.Error);
            }));

            _subscriptions.Add(context.On(events.ReceiveStreamEnd, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                Complete();
            }));
        }

        private IAsyncEnumerable<TResponse> CreateResultStream(Func<ValueTask> onDispose)
        {
            return _responses.ReadAll(onDispose: onDispose);
        }

        private void Cleanup()
        {
            foreach (var subscription in _subscriptions)
            {
                subscription.Dispose();
            }
        }

        private void Finish(Exception? error, bool emitAbort)
        {
            if (Interlocked.Exchange(ref _finished, 1) != 0) return;

            _requestCancellationSource.Cancel();

            if (emitAbort)
            {
                context.Emit(events.SendAbort, new AbortPayload(_invokeId));
            }

            if (error is null)
            {
                _responses.Complete();
            }
            else
            {
                _responses.Fault(error);
            }

            Cleanup();
            _requestCancellationSource.Dispose();
        }

        private void Complete()
        {
            Finish(error: null, emitAbort: false);
        }

        private void Fault(Exception error)
        {
            Finish(error, emitAbort: false);
        }

        private void AbortFromClient()
        {
            Abort(new OperationCanceledException(cancellationToken));
        }

        private void Abort(Exception? error)
        {
            Finish(error, emitAbort: true);
        }

        private void DispatchSendRequest()
        {
            if (sendDispatchMode is SendDispatchMode.InlineAfterCancellationArmed)
            {
                // Keep unary request dispatch inline once cancellation is armed so
                // an early abort or enumerator disposal cannot be overtaken by a
                // queued Task.Run send that starts the handler afterward.
                ExecuteSendRequestAsync().GetAwaiter().GetResult();
                return;
            }

            _ = Task.Run(ExecuteSendRequestAsync, CancellationToken.None);
        }

        private async Task ExecuteSendRequestAsync()
        {
            try
            {
                await sendRequest(_invokeId, _requestCancellationSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_requestCancellationSource.Token.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                Fault(error);
            }
        }

        private ValueTask AbortOnDisposeAsync()
        {
            Abort(null);
            return ValueTask.CompletedTask;
        }

        private static ValueTask NoopOnDisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    // Support

    private enum SendDispatchMode
    {
        InlineAfterCancellationArmed,
        QueueOnThreadPool,
    }
}
