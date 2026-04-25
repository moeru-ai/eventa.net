namespace Eventa;

public sealed class InvokeStreamClient<TResponse, TRequest>
{
    private readonly Func<IEventContext> _contextFactory;
    private readonly InvokeEventBindings<TResponse, TRequest> _events;

    internal InvokeStreamClient(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        _contextFactory = contextFactory;
        _events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
    }

    public IAsyncEnumerable<TResponse> InvokeAsync(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        var context = _contextFactory();

        return new PendingStreamInvokeOperation(
            context,
            _events,
            cancellationToken,
            sendDispatchMode: SendDispatchMode.InlineAfterCancellationArmed,
            (invokeId, requestCancellationToken) =>
            {
                if (requestCancellationToken.IsCancellationRequested)
                {
                    return Task.CompletedTask;
                }

                context.Emit(_events.Send, new SendPayload<TRequest>(invokeId, request));
                return Task.CompletedTask;
            }).Run();
    }

    public IAsyncEnumerable<TResponse> InvokeAsync(
        IAsyncEnumerable<TRequest> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var context = _contextFactory();

        return new PendingStreamInvokeOperation(
            context,
            _events,
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

                        context.Emit(_events.Send, new SendPayload<TRequest>(invokeId, item));
                    }
                }
                catch (OperationCanceledException) when (requestCancellationToken.IsCancellationRequested) { return; }

                if (requestCancellationToken.IsCancellationRequested) return;

                context.Emit(_events.SendStreamEnd, new StreamEndPayload(invokeId));
            }).Run();
    }

    private sealed class PendingStreamInvokeOperation(
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
            _subscriptions.Add(context.Subscribe(events.Receive, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                _responses.TryWrite(envelope.Body.Content);
            }));

            _subscriptions.Add(context.Subscribe(events.ReceiveError, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                Fault(envelope.Body.Error);
            }));

            _subscriptions.Add(context.Subscribe(events.ReceiveStreamEnd, envelope =>
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

    private enum SendDispatchMode
    {
        InlineAfterCancellationArmed,
        QueueOnThreadPool,
    }
}
