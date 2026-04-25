namespace Eventa;

/// <summary>
/// Wires the four supported handler shapes onto the invoke protocol matrix:
/// unary request or request stream, each with unary or stream responses.
/// </summary>
internal static class InvokeHandlerRegistrationFactory
{
    #region Unary Request

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
                cancellationSource.Token,
                responses).ConfigureAwait(false);
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
                state.CancellationSource.Token,
                responses).ConfigureAwait(false);
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

    private static void EmitReceiveError<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        string invokeId,
        Exception error)
    {
        context.Emit(events.ReceiveError, new ReceiveErrorPayload(invokeId, error));
    }

    private static async Task ForwardStreamResponsesAsync<TResponse, TRequest>(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        string invokeId,
        CancellationToken cancellationToken,
        IAsyncEnumerable<TResponse> responses)
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
