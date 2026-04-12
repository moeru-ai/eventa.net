namespace Eventa;

public static class EventInvoke
{
    public static Func<TRequest, CancellationToken, Task<TResponse>> DefineInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        throw new NotImplementedException();
    }

    public static Func<TRequest, CancellationToken, Task<TResponse>> DefineInvoke<TResponse, TRequest>(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        throw new NotImplementedException();
    }

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        throw new NotImplementedException();
    }

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        throw new NotImplementedException();
    }
}
