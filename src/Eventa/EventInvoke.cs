using System.Runtime.CompilerServices;

namespace Eventa;

public static class EventInvoke
{
    private static readonly ConditionalWeakTable<IEventContext, InvokeHandlerRegistry> HandlerRegistries = [];

    #region Client Invoke API

    public static InvokeClient<TResponse, TRequest> CreateInvokeClient<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return CreateInvokeClient(() => context, eventDefinition);
    }

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
