namespace Eventa;

public static class EventStream
{
    #region Client Invoke API

    public static InvokeStreamClient<TResponse, TRequest> CreateInvokeStreamClient<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return CreateInvokeStreamClient(() => context, eventDefinition);
    }

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

    public static IDisposable RegisterStreamHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return StreamHandlerRegistrationFactory.CreateUnary(context, eventDefinition, handler);
    }

    public static IDisposable RegisterStreamHandler<TResponse, TRequest>(
        this IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return StreamHandlerRegistrationFactory.CreateRequestStream(context, eventDefinition, handler);
    }

    #endregion

    #region Handler Adapters

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
