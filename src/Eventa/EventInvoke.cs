using System.Runtime.CompilerServices;

namespace Eventa;

/// <summary>
/// Exposes client and handler helpers for invoke contracts that complete with a single response.
/// </summary>
/// <remarks>
/// These helpers support unary requests and request streams, but the response side always resolves
/// to one value or one terminal error. Use <see cref="EventStream"/> when the caller should read
/// a stream of response items instead.
/// </remarks>
public static class EventInvoke
{
    private static readonly ConditionalWeakTable<IEventContext, InvokeHandlerRegistry> HandlerRegistries = [];

    #region Client Invoke API

    /// <summary>
    /// Creates a unary invoke client bound to the supplied context and invoke definition.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that will send invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <returns>A reusable client that can issue invokes against the contract.</returns>
    public static InvokeClient<TResponse, TRequest> CreateInvokeClient<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return CreateInvokeClient(() => context, eventDefinition);
    }

    /// <summary>
    /// Creates a unary invoke client that resolves its context lazily for each call.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="contextFactory">A factory that supplies the context used for each invoke.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <returns>A reusable client that can issue invokes against the contract.</returns>
    public static InvokeClient<TResponse, TRequest> CreateInvokeClient<TResponse, TRequest>(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return new InvokeClient<TResponse, TRequest>(contextFactory, eventDefinition);
    }

    #endregion

    #region Handler Registration API

    /// <summary>
    /// Registers a unary-request, unary-response handler for the supplied invoke contract.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
    /// <param name="context">The context that should receive invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <param name="handler">
    /// The asynchronous handler that receives one request payload and returns one response payload.
    /// </param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes the handler registration and cleans up inflight state.
    /// </returns>
    public static IDisposable RegisterInvokeHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return HandlerRegistries
            .GetValue(context, static _ => new InvokeHandlerRegistry())
            .Register(
                eventDefinition.SendEventId,
                handler,
                () => InvokeHandlerRegistrationFactory.CreateUnary(context, eventDefinition, handler));
    }

    /// <summary>
    /// Registers a request-stream, unary-response handler for the supplied invoke contract.
    /// </summary>
    /// <typeparam name="TResponse">The response payload type returned by the handler.</typeparam>
    /// <typeparam name="TRequest">The request payload type produced by the caller stream.</typeparam>
    /// <param name="context">The context that should receive invoke protocol events.</param>
    /// <param name="eventDefinition">The invoke contract that defines the protocol event ids.</param>
    /// <param name="handler">
    /// The asynchronous handler that consumes the caller's request stream and returns one response.
    /// </param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes the handler registration and cleans up inflight state.
    /// </returns>
    public static IDisposable RegisterInvokeHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return HandlerRegistries
            .GetValue(context, static _ => new InvokeHandlerRegistry())
            .Register(
                eventDefinition.SendEventId,
                handler,
                () => InvokeHandlerRegistrationFactory.CreateRequestStream(context, eventDefinition, handler));
    }

    #endregion
}
