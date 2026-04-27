namespace Eventa;

/// <summary>
/// Wires the four supported handler shapes onto the invoke protocol matrix:
/// unary request or request stream, each with unary or stream responses.
/// </summary>
/// <remarks>
/// The factory materializes concrete protocol subscriptions and the matching inflight-state
/// tracker for each handler shape so registration sites can stay small and delegate the protocol
/// choreography to one implementation surface.
/// </remarks>
internal static class InvokeHandlerRegistrationFactory
{
    #region Unary Request

    /// <summary>
    /// Creates the registration for a unary-request, unary-response invoke handler.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="eventDefinition">The invoke contract whose derived event ids should be wired.</param>
    /// <param name="handler">The handler that receives one request payload and returns one response.</param>
    /// <returns>
    /// A registration that owns the protocol subscriptions and inflight cancellation cleanup.
    /// </returns>
    public static HandlerRegistration CreateUnary<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new InvocationCancellationTracker();

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var session = new UnaryRequestHandlerSession<TResponse, TRequest>(
                context,
                events,
                inflight,
                invokeId);

            await session.RunAsync(async cancellationToken =>
            {
                var response = await handler(request, cancellationToken).ConfigureAwait(false);
                session.EmitReceiveIfActive(response, cancellationToken);
            }).ConfigureAwait(false);
        }

        return new HandlerRegistration(
            CreateUnaryRequestSubscriptions(context, events, HandleInvokeAsync, inflight.TryCancel),
            inflight.CancelAllAndDispose);
    }

    /// <summary>
    /// Creates the registration for a unary-request, stream-response invoke handler.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="eventDefinition">The invoke contract whose derived event ids should be wired.</param>
    /// <param name="handler">The handler that receives one request payload and yields a response stream.</param>
    /// <returns>
    /// A registration that owns the protocol subscriptions and inflight cancellation cleanup.
    /// </returns>
    public static HandlerRegistration CreateUnary<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new InvocationCancellationTracker();

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var session = new UnaryRequestHandlerSession<TResponse, TRequest>(
                context,
                events,
                inflight,
                invokeId);

            await session.RunAsync(cancellationToken => session.ForwardStreamResponsesAsync(
                handler(request, cancellationToken),
                cancellationToken)).ConfigureAwait(false);
        }

        return new HandlerRegistration(
            CreateUnaryRequestSubscriptions(context, events, HandleInvokeAsync, inflight.TryCancel),
            inflight.CancelAllAndDispose);
    }

    /// <summary>
    /// Creates the shared protocol subscriptions for unary-request handlers.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type carried by the invoke contract.</typeparam>
    /// <typeparam name="TRequest">The request payload type carried by the invoke contract.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="handleInvokeAsync">The callback that starts handler execution for a request.</param>
    /// <param name="tryCancel">The callback that cancels an inflight request when an abort arrives.</param>
    /// <returns>The subscriptions required to receive request and abort events.</returns>
    private static List<IDisposable> CreateUnaryRequestSubscriptions<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        Func<string, TRequest, Task> handleInvokeAsync,
        Action<string> tryCancel)
    {
        return
        [
            context.Subscribe(events.Send, envelope => _ = handleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.Subscribe(events.SendAbort, envelope => tryCancel(envelope.Body.InvokeId)),
        ];
    }

    #endregion

    #region Request Stream

    /// <summary>
    /// Creates the registration for a request-stream, unary-response invoke handler.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type yielded by the caller stream.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="eventDefinition">The invoke contract whose derived event ids should be wired.</param>
    /// <param name="handler">The handler that consumes a request stream and returns one response.</param>
    /// <returns>
    /// A registration that owns the protocol subscriptions and inflight request-stream cleanup.
    /// </returns>
    public static HandlerRegistration CreateRequestStream<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new RequestStreamInvocationTracker<TRequest>();

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            var session = new RequestStreamHandlerSession<TResponse, TRequest>(
                context,
                events,
                inflight,
                state);

            await session.RunAsync(async (request, cancellationToken) =>
            {
                var response = await handler(request, cancellationToken).ConfigureAwait(false);
                session.EmitReceiveIfActive(response, cancellationToken);
            }).ConfigureAwait(false);
        }

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            return inflight.GetOrCreate(invokeId, HandleInvokeAsync);
        }

        return new HandlerRegistration(
            CreateRequestStreamSubscriptions(context, events, GetOrCreateState),
            inflight.AbortAllAndDispose);
    }

    /// <summary>
    /// Creates the registration for a request-stream, stream-response invoke handler.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type yielded by the caller stream.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="eventDefinition">The invoke contract whose derived event ids should be wired.</param>
    /// <param name="handler">The handler that consumes a request stream and yields a response stream.</param>
    /// <returns>
    /// A registration that owns the protocol subscriptions and inflight request-stream cleanup.
    /// </returns>
    public static HandlerRegistration CreateRequestStream<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new RequestStreamInvocationTracker<TRequest>();

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            var session = new RequestStreamHandlerSession<TResponse, TRequest>(
                context,
                events,
                inflight,
                state);

            await session.RunAsync((request, cancellationToken) => session.ForwardStreamResponsesAsync(
                handler(request, cancellationToken),
                cancellationToken)).ConfigureAwait(false);
        }

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            return inflight.GetOrCreate(invokeId, HandleInvokeAsync);
        }

        return new HandlerRegistration(
            CreateRequestStreamSubscriptions(context, events, GetOrCreateState),
            inflight.AbortAllAndDispose);
    }

    /// <summary>
    /// Creates the shared protocol subscriptions for request-stream handlers.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type carried by the invoke contract.</typeparam>
    /// <typeparam name="TRequest">The request payload type yielded by the caller stream.</typeparam>
    /// <param name="context">The context that should receive the invoke protocol traffic.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="getOrCreateState">The callback that resolves the mutable state for an invoke id.</param>
    /// <returns>The subscriptions required to receive request items, stream-end, and abort events.</returns>
    private static List<IDisposable> CreateRequestStreamSubscriptions<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        Func<string, RequestStreamInvocationState<TRequest>> getOrCreateState)
    {
        return
        [
            context.Subscribe(events.Send, envelope =>
            {
                getOrCreateState(envelope.Body.InvokeId).Requests.TryWrite(envelope.Body.Content);
            }),
            context.Subscribe(events.SendStreamEnd, envelope =>
            {
                // Keep empty request streams as a supported C# contract.
                getOrCreateState(envelope.Body.InvokeId).Requests.Complete();
            }),
            context.Subscribe(events.SendAbort, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract.
                getOrCreateState(envelope.Body.InvokeId).Abort();
            }),
        ];
    }

    #endregion

    #region Handler Sessions

    /// <summary>
    /// Carries shared protocol helpers for one handler-side invoke execution.
    /// </summary>
    private abstract class InvokeHandlerSession<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        string invokeId)
    {
        private readonly IEventContext _context = context;
        private readonly InvokeEventBindings<TResponse, TRequest> _events = events;

        protected string InvokeId { get; } = invokeId;

        public void EmitReceiveIfActive(TResponse response, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) return;

            _context.Emit(_events.Receive, new ReceivePayload<TResponse>(InvokeId, response));
        }

        public async Task ForwardStreamResponsesAsync(
            IAsyncEnumerable<TResponse> responses,
            CancellationToken cancellationToken)
        {
            // Avoid starting handler-stream enumeration after an early abort won.
            if (cancellationToken.IsCancellationRequested) return;

            await foreach (var item in responses.ConfigureAwait(false))
            {
                // Cancellation can win after MoveNextAsync but before we emit this item.
                if (cancellationToken.IsCancellationRequested) return;

                _context.Emit(_events.Receive, new ReceivePayload<TResponse>(InvokeId, item));
            }

            // Do not send stream-end if cancellation arrived after the last item.
            if (cancellationToken.IsCancellationRequested) return;

            _context.Emit(_events.ReceiveStreamEnd, new StreamEndPayload(InvokeId));
        }

        protected void EmitReceiveError(Exception error)
        {
            _context.Emit(_events.ReceiveError, new ReceiveErrorPayload(InvokeId, error));
        }
    }

    /// <summary>
    /// Runs the shared lifecycle for one unary-request handler execution.
    /// </summary>
    private sealed class UnaryRequestHandlerSession<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        InvocationCancellationTracker inflight,
        string invokeId) : InvokeHandlerSession<TResponse, TRequest>(context, events, invokeId)
    {
        private readonly InvocationCancellationTracker _inflight = inflight;

        public async Task RunAsync(Func<CancellationToken, Task> executeAsync)
        {
            var cancellationSource = _inflight.BeginTracking(InvokeId);

            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (cancellationSource.IsCancellationRequested) return;

                await executeAsync(cancellationSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (cancellationSource.IsCancellationRequested) return;

                EmitReceiveError(error);
            }
            finally
            {
                _inflight.StopTracking(InvokeId, cancellationSource);
                cancellationSource.Dispose();
            }
        }
    }

    /// <summary>
    /// Runs the shared lifecycle for one request-stream handler execution.
    /// </summary>
    private sealed class RequestStreamHandlerSession<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        RequestStreamInvocationTracker<TRequest> inflight,
        RequestStreamInvocationState<TRequest> state) : InvokeHandlerSession<TResponse, TRequest>(context, events, state.InvokeId)
    {
        private readonly RequestStreamInvocationTracker<TRequest> _inflight = inflight;
        private readonly RequestStreamInvocationState<TRequest> _state = state;

        public async Task RunAsync(Func<IAsyncEnumerable<TRequest>, CancellationToken, Task> executeAsync)
        {
            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (_state.CancellationSource.IsCancellationRequested) return;

                await executeAsync(
                    _state.Requests.ReadAll(respectConsumerCancellation: false),
                    _state.CancellationSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_state.CancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (_state.CancellationSource.IsCancellationRequested) return;

                EmitReceiveError(error);
            }
            finally
            {
                _inflight.Remove(_state);
                _state.Dispose();
            }
        }
    }

    #endregion
}
