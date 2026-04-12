namespace Eventa;

public static class EventStream
{
    public static IAsyncEnumerable<TResponse> DefineStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public static IAsyncEnumerable<TResponse> DefineStreamInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        IAsyncEnumerable<TRequest> request,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        throw new NotImplementedException();
    }

    public static IDisposable DefineStreamInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        throw new NotImplementedException();
    }

    public static Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> ToStreamHandler<TResponse, TRequest>(
        Func<TRequest, Func<TResponse, ValueTask>, CancellationToken, Task> handler)
    {
        throw new NotImplementedException();
    }
}
