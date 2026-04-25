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

        Task HandleInvokeAsync(string invokeId, TRequest request) =>
            RunUnaryRequestUnaryResponseHandlerAsync(context, events, inflight, invokeId, request, handler);

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

        Task HandleInvokeAsync(string invokeId, TRequest request) =>
            RunUnaryRequestStreamResponseHandlerAsync(context, events, inflight, invokeId, request, handler);

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

    /// <summary>
    /// Runs one unary-request, unary-response handler invocation and emits the terminal response or error.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context used to emit response protocol events.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="inflight">The cancellation tracker that owns this invoke's cancellation source.</param>
    /// <param name="invokeId">The protocol invoke id for this handler execution.</param>
    /// <param name="request">The request payload passed to the handler.</param>
    /// <param name="handler">The handler to execute.</param>
    private static async Task RunUnaryRequestUnaryResponseHandlerAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        InvocationCancellationTracker inflight,
        string invokeId,
        TRequest request,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        var cancellationSource = inflight.BeginTracking(invokeId);

        try
        {
            // Skip handler activation entirely when abort won before startup.
            if (cancellationSource.IsCancellationRequested) return;

            var response = await handler(request, cancellationSource.Token).ConfigureAwait(false);

            // Cancellation can still win while the handler is awaiting.
            if (cancellationSource.IsCancellationRequested) return;

            context.Emit(events.Receive, new ReceivePayload<TResponse>(invokeId, response));
        }
        catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
        catch (Exception error)
        {
            if (cancellationSource.IsCancellationRequested) return;

            EmitReceiveError(context, events, invokeId, error);
        }
        finally
        {
            inflight.StopTracking(invokeId);
            cancellationSource.Dispose();
        }
    }

    /// <summary>
    /// Runs one unary-request, stream-response handler invocation and forwards its response stream.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context used to emit response protocol events.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="inflight">The cancellation tracker that owns this invoke's cancellation source.</param>
    /// <param name="invokeId">The protocol invoke id for this handler execution.</param>
    /// <param name="request">The request payload passed to the handler.</param>
    /// <param name="handler">The handler to execute.</param>
    private static async Task RunUnaryRequestStreamResponseHandlerAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        InvocationCancellationTracker inflight,
        string invokeId,
        TRequest request,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        var cancellationSource = inflight.BeginTracking(invokeId);

        try
        {
            // Skip handler activation entirely when abort won before startup.
            if (cancellationSource.IsCancellationRequested) return;

            var responses = handler(request, cancellationSource.Token);
            await ForwardStreamResponsesAsync(
                context,
                events,
                invokeId,
                responses,
                cancellationSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
        catch (Exception error)
        {
            if (cancellationSource.IsCancellationRequested) return;

            EmitReceiveError(context, events, invokeId, error);
        }
        finally
        {
            inflight.StopTracking(invokeId);
            cancellationSource.Dispose();
        }
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

        Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state) =>
            RunRequestStreamUnaryResponseHandlerAsync(context, events, inflight, state, handler);

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

        Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state) =>
            RunRequestStreamStreamResponseHandlerAsync(context, events, inflight, state, handler);

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

    /// <summary>
    /// Runs one request-stream, unary-response handler invocation and emits the terminal response or error.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type yielded by the caller stream.</typeparam>
    /// <param name="context">The context used to emit response protocol events.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="inflight">The tracker that owns mutable per-invoke request-stream state.</param>
    /// <param name="state">The mutable state for this specific invoke.</param>
    /// <param name="handler">The handler to execute.</param>
    private static async Task RunRequestStreamUnaryResponseHandlerAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        RequestStreamInvocationTracker<TRequest> inflight,
        RequestStreamInvocationState<TRequest> state,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        try
        {
            // Skip handler activation entirely when abort won before startup.
            if (state.CancellationSource.IsCancellationRequested) return;

            var response = await handler(
                state.Requests.ReadAll(respectConsumerCancellation: false),
                state.CancellationSource.Token).ConfigureAwait(false);

            // Cancellation can still win while the handler is awaiting.
            if (state.CancellationSource.IsCancellationRequested) return;

            context.Emit(events.Receive, new ReceivePayload<TResponse>(state.InvokeId, response));
        }
        catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested) { return; }
        catch (Exception error)
        {
            if (state.CancellationSource.IsCancellationRequested) return;

            EmitReceiveError(context, events, state.InvokeId, error);
        }
        finally
        {
            inflight.Remove(state);
            state.Dispose();
        }
    }

    /// <summary>
    /// Runs one request-stream, stream-response handler invocation and forwards its response stream.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type yielded by the caller stream.</typeparam>
    /// <param name="context">The context used to emit response protocol events.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="inflight">The tracker that owns mutable per-invoke request-stream state.</param>
    /// <param name="state">The mutable state for this specific invoke.</param>
    /// <param name="handler">The handler to execute.</param>
    private static async Task RunRequestStreamStreamResponseHandlerAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        RequestStreamInvocationTracker<TRequest> inflight,
        RequestStreamInvocationState<TRequest> state,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        try
        {
            // Skip handler activation entirely when abort won before startup.
            if (state.CancellationSource.IsCancellationRequested) return;

            var responses = handler(
                state.Requests.ReadAll(respectConsumerCancellation: false),
                state.CancellationSource.Token);
            await ForwardStreamResponsesAsync(
                context,
                events,
                state.InvokeId,
                responses,
                state.CancellationSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested) { return; }
        catch (Exception error)
        {
            if (state.CancellationSource.IsCancellationRequested) return;

            EmitReceiveError(context, events, state.InvokeId, error);
        }
        finally
        {
            inflight.Remove(state);
            state.Dispose();
        }
    }

    #endregion

    #region Shared Helpers

    /// <summary>
    /// Emits the invoke contract's response-error event for a failed handler execution.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type carried by the invoke contract.</typeparam>
    /// <typeparam name="TRequest">The request payload type carried by the invoke contract.</typeparam>
    /// <param name="context">The context used to emit the error event.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="invokeId">The protocol invoke id that failed.</param>
    /// <param name="error">The terminal error raised by the handler.</param>
    private static void EmitReceiveError<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        string invokeId,
        Exception error)
    {
        context.Emit(events.ReceiveError, new ReceiveErrorPayload(invokeId, error));
    }

    /// <summary>
    /// Forwards a handler-produced response stream onto the invoke protocol until completion or cancellation.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type carried by the invoke contract.</typeparam>
    /// <param name="context">The context used to emit response protocol events.</param>
    /// <param name="events">The concrete protocol event bindings for the invoke contract.</param>
    /// <param name="invokeId">The protocol invoke id that owns the response stream.</param>
    /// <param name="responses">The response stream yielded by the handler.</param>
    /// <param name="cancellationToken">The token that aborts forwarding when the invoke is canceled.</param>
    private static async Task ForwardStreamResponsesAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        string invokeId,
        IAsyncEnumerable<TResponse> responses,
        CancellationToken cancellationToken)
    {
        // Avoid starting handler-stream enumeration after an early abort won.
        if (cancellationToken.IsCancellationRequested) return;

        await foreach (var item in responses.ConfigureAwait(false))
        {
            // Cancellation can win after MoveNextAsync but before we emit this item.
            if (cancellationToken.IsCancellationRequested) return;

            context.Emit(events.Receive, new ReceivePayload<TResponse>(invokeId, item));
        }

        // Do not send stream-end if cancellation arrived after the last item.
        if (cancellationToken.IsCancellationRequested) return;

        context.Emit(events.ReceiveStreamEnd, new StreamEndPayload(invokeId));
    }

    #endregion
}
