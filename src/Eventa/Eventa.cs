namespace Eventa;

public static class Eventa
{
    public static EventDefinition<TPayload> Define<TPayload>(string? id = null)
    {
        return new EventDefinition<TPayload>(id ?? IdGenerator.New());
    }

    public static InvokeEventDefinition<TResponse, TRequest> DefineInvoke<TResponse, TRequest>(
        string? tag = null)
    {
        return new InvokeEventDefinition<TResponse, TRequest>(tag ?? IdGenerator.New());
    }
}
