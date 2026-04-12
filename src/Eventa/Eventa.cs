namespace Eventa;

public static class Eventa
{
    public static EventDefinition<TPayload> Define<TPayload>(string? id = null)
    {
        throw new NotImplementedException();
    }

    public static InvokeEventDefinition<TResponse, TRequest> DefineInvoke<TResponse, TRequest>(
        string? tag = null)
    {
        throw new NotImplementedException();
    }
}
