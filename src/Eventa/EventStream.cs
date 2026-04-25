namespace Eventa;

/// <summary>
/// Exposes client and handler helpers for invoke contracts that return streamed responses.
/// </summary>
/// <remarks>
/// These helpers cover server-streaming and bidirectional-streaming shapes. The request side can
/// stay unary or be modeled as <see cref="IAsyncEnumerable{T}"/>, while the response side is always
/// consumed as an async sequence.
/// </remarks>
public static class EventStream
{
    #region Client Invoke API

    /// <summary>
    /// Creates a streaming invoke client bound to the supplied context and invoke definition.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the stream.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that will send and receive invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <returns>A reusable client that can issue streaming invokes against the contract.</returns>
    public static InvokeStreamClient<TResponse, TRequest> CreateInvokeStreamClient<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return CreateInvokeStreamClient(() => context, eventDefinition);
    }

    /// <summary>
    /// Creates a streaming invoke client that resolves its context lazily for each call.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the stream.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="contextFactory">A factory that supplies the context used for each invoke.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <returns>A reusable client that can issue streaming invokes against the contract.</returns>
    public static InvokeStreamClient<TResponse, TRequest> CreateInvokeStreamClient<TResponse, TRequest>(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return new InvokeStreamClient<TResponse, TRequest>(contextFactory, eventDefinition);
    }

    #endregion

    #region Handler Registration API

    /// <summary>
    /// Registers a unary-request, stream-response handler for the supplied invoke contract.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The unary request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that should receive invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <param name="handler">
    /// The handler that receives one request payload and yields zero or more response items.
    /// </param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes the handler registration and cleans up inflight state.
    /// </returns>
    public static IDisposable RegisterStreamHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return InvokeHandlerRegistrationFactory.CreateUnary(context, eventDefinition, handler);
    }

    /// <summary>
    /// Registers a request-stream, stream-response handler for the supplied invoke contract.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type yielded by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type produced by the caller stream.</typeparam>
    /// <param name="context">The context that should receive invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <param name="handler">
    /// The handler that consumes the caller's request stream and yields zero or more response items.
    /// </param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes the handler registration and cleans up inflight state.
    /// </returns>
    public static IDisposable RegisterStreamHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return InvokeHandlerRegistrationFactory.CreateRequestStream(context, eventDefinition, handler);
    }

    #endregion

    #region Handler Adapters

    /// <summary>
    /// Adapts a callback-driven streaming handler to Eventa's async-sequence handler shape.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type emitted by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type accepted by the handler.</typeparam>
    /// <param name="handler">
    /// The callback-based handler that pushes responses through the provided writer callback.
    /// </param>
    /// <returns>
    /// A stream handler compatible with <see cref="RegisterStreamHandler{TResponse, TRequest}(IEventContext, InvokeEventDefinition{TResponse, TRequest}, Func{TRequest, CancellationToken, IAsyncEnumerable{TResponse}})"/>.
    /// </returns>
    /// <remarks>
    /// Response items are buffered in an <see cref="AsyncSignalQueue{T}"/>, so completion and
    /// faults flow to the caller through normal async-enumerator semantics.
    /// </remarks>
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

    #endregion
}
