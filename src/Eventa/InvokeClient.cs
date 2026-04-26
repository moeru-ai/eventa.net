namespace Eventa;

/// <summary>
/// Represents a reusable client for invoke contracts that complete with a single response.
/// </summary>
/// <typeparam name="TResponse">The response payload type returned by each invoke.</typeparam>
/// <typeparam name="TRequest">The request payload type sent by each invoke.</typeparam>
/// <remarks>
/// Each call allocates a fresh invoke id, subscribes to the derived response channels for that id,
/// and completes when a response, protocol error, cancellation, or fatal adapter event arrives.
/// </remarks>
public sealed class InvokeClient<TResponse, TRequest>
{
    private readonly Func<IEventContext> _contextFactory;
    private readonly InvokeEventBindings<TResponse, TRequest> _events;

    internal InvokeClient(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        _contextFactory = contextFactory;
        _events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
    }

    /// <summary>
    /// Sends one request and awaits one terminal response for the bound invoke contract.
    /// </summary>
    /// <param name="request">The request payload to send to the handler.</param>
    /// <param name="cancellationToken">
    /// A token that aborts the invoke locally and emits the protocol abort event when canceled.
    /// </param>
    /// <returns>A task that resolves to the handler response payload.</returns>
    public Task<TResponse> InvokeAsync(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        var context = _contextFactory();

        return new UnaryInvokeSessionEngine<TResponse, TRequest>(
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
    /// Sends a request stream and awaits one terminal response for the bound invoke contract.
    /// </summary>
    /// <param name="request">The async sequence whose items should be forwarded as request payloads.</param>
    /// <param name="cancellationToken">
    /// A token that aborts the invoke locally and emits the protocol abort event when canceled.
    /// </param>
    /// <returns>A task that resolves to the handler response payload.</returns>
    public Task<TResponse> InvokeAsync(
        IAsyncEnumerable<TRequest> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var context = _contextFactory();

        return new UnaryInvokeSessionEngine<TResponse, TRequest>(
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
}

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

        return new StreamInvokeSessionEngine<TResponse, TRequest>(
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

        return new StreamInvokeSessionEngine<TResponse, TRequest>(
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
}
