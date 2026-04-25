namespace Eventa;

/// <summary>
/// Represents a reusable client for invoke contracts that return streamed responses.
/// </summary>
/// <typeparam name="TResponse">The response payload type yielded by each invoke stream.</typeparam>
/// <typeparam name="TRequest">The request payload type sent by each invoke.</typeparam>
/// <remarks>
/// Each invocation allocates a fresh invoke id and exposes the response side as an
/// <see cref="IAsyncEnumerable{T}"/>. Canceling the operation or disposing enumeration early sends
/// the protocol abort event to the remote handler.
/// </remarks>
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

    /// <summary>
    /// Sends one request payload and returns the streamed responses for the invoke.
    /// </summary>
    /// <param name="request">The unary request payload to send to the handler.</param>
    /// <param name="cancellationToken">
    /// A token that aborts the invoke locally and emits the protocol abort event when canceled.
    /// </param>
    /// <returns>An async sequence that yields response items for this invoke.</returns>
    public IAsyncEnumerable<TResponse> InvokeAsync(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        var context = _contextFactory();

        return new PendingStreamInvokeOperation(
            context,
            _events,
            sendDispatchMode: SendDispatchMode.InlineAfterCancellationArmed,
            (invokeId, requestCancellationToken) =>
            {
                if (requestCancellationToken.IsCancellationRequested)
                {
                    return Task.CompletedTask;
                }

                context.Emit(_events.Send, new SendPayload<TRequest>(invokeId, request));
                return Task.CompletedTask;
            },
            cancellationToken).Run();
    }

    /// <summary>
    /// Sends a request stream and returns the streamed responses for the invoke.
    /// </summary>
    /// <param name="request">The async sequence whose items should be forwarded as request payloads.</param>
    /// <param name="cancellationToken">
    /// A token that aborts the invoke locally and emits the protocol abort event when canceled.
    /// </param>
    /// <returns>An async sequence that yields response items for this invoke.</returns>
    public IAsyncEnumerable<TResponse> InvokeAsync(
        IAsyncEnumerable<TRequest> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var context = _contextFactory();

        return new PendingStreamInvokeOperation(
            context,
            _events,
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
            },
            cancellationToken).Run();
    }

    /// <summary>
    /// Encapsulates one pending stream invoke from subscription setup through terminal cleanup.
    /// </summary>
    /// <param name="context">The context that carries the invoke protocol traffic.</param>
    /// <param name="events">The concrete protocol events for the invoke contract.</param>
    /// <param name="sendDispatchMode">Controls whether request dispatch runs inline or on the thread pool.</param>
    /// <param name="sendRequest">The callback that emits the unary request or pumps the request stream.</param>
    /// <param name="cancellationToken">The client token that can abort the invoke.</param>
    private sealed class PendingStreamInvokeOperation(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        SendDispatchMode sendDispatchMode,
        Func<string, CancellationToken, Task> sendRequest,
        CancellationToken cancellationToken)
    {
        private readonly string _invokeId = IdGenerator.New();
        private readonly AsyncSignalQueue<TResponse> _responses = new();
        private readonly CancellationTokenSource _requestCancellationSource = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : new CancellationTokenSource();
        private readonly List<IDisposable> _subscriptions = [];
        private int _finished;

        /// <summary>
        /// Arms protocol subscriptions, wires cancellation, dispatches the request, and returns the response stream.
        /// </summary>
        /// <returns>An async sequence that yields responses until the invoke completes or faults.</returns>
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

        /// <summary>
        /// Subscribes the invoke-specific response, error, and stream-end channels.
        /// </summary>
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

        /// <summary>
        /// Exposes the buffered responses as an async sequence with invoke-specific disposal behavior.
        /// </summary>
        /// <param name="onDispose">The callback to run when the consumer stops enumeration early.</param>
        /// <returns>An async sequence over the buffered response items.</returns>
        private IAsyncEnumerable<TResponse> CreateResultStream(Func<ValueTask> onDispose)
        {
            return _responses.ReadAll(onDispose: onDispose);
        }

        /// <summary>
        /// Disposes every subscription owned by this pending stream invoke.
        /// </summary>
        private void Cleanup()
        {
            foreach (var subscription in _subscriptions)
            {
                subscription.Dispose();
            }
        }

        /// <summary>
        /// Transitions the invoke stream to its terminal state once and optionally emits a protocol abort.
        /// </summary>
        /// <param name="error">
        /// The terminal error to surface to the consumer, or <see langword="null"/> for normal completion.
        /// </param>
        /// <param name="emitAbort">
        /// <see langword="true"/> to notify the remote side that the client aborted the invoke.
        /// </param>
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

        /// <summary>
        /// Completes the response stream normally.
        /// </summary>
        private void Complete()
        {
            Finish(error: null, emitAbort: false);
        }

        /// <summary>
        /// Faults the response stream with a protocol or transport error.
        /// </summary>
        /// <param name="error">The exception that should terminate the response stream.</param>
        private void Fault(Exception error)
        {
            Finish(error, emitAbort: false);
        }

        /// <summary>
        /// Aborts the invoke from the client token and surfaces cancellation to the consumer.
        /// </summary>
        private void AbortFromClient()
        {
            Abort(new OperationCanceledException(cancellationToken));
        }

        /// <summary>
        /// Aborts the invoke and optionally surfaces a terminal error to the consumer.
        /// </summary>
        /// <param name="error">
        /// The terminal error to surface to the consumer, or <see langword="null"/> when disposal
        /// should end the stream via a protocol abort without a local exception.
        /// </param>
        private void Abort(Exception? error)
        {
            Finish(error, emitAbort: true);
        }

        /// <summary>
        /// Starts request dispatch inline or on the thread pool according to the configured mode.
        /// </summary>
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

        /// <summary>
        /// Executes the request-dispatch callback and converts send failures into terminal stream faults.
        /// </summary>
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

        /// <summary>
        /// Aborts the invoke when the response stream consumer stops enumeration before completion.
        /// </summary>
        private ValueTask AbortOnDisposeAsync()
        {
            Abort(null);
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Leaves the invoke untouched when cleanup already happened before enumeration was exposed.
        /// </summary>
        private static ValueTask NoopOnDisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Controls whether request dispatch runs inline or is queued to the thread pool.
    /// </summary>
    private enum SendDispatchMode
    {
        /// <summary>
        /// Dispatches the request inline once cancellation is armed to avoid queued-send races.
        /// </summary>
        InlineAfterCancellationArmed,

        /// <summary>
        /// Queues request pumping to the thread pool, which is suitable for streamed request sources.
        /// </summary>
        QueueOnThreadPool,
    }
}
