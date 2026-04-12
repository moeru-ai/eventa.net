namespace Eventa;

public sealed class EventContext(IEventaAdapter? adapter = null) : IEventContext
{
    public IDictionary<string, object> Extensions { get; } = new Dictionary<string, object>();

    public IEventaAdapter? Adapter { get; } = adapter;

    public static IEventContext Create(IEventaAdapter? adapter = null)
    {
        throw new NotImplementedException();
    }

    public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
    {
        throw new NotImplementedException();
    }

    public void Emit<TPayload, TOptions>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        TOptions options)
        where TOptions : class
    {
        throw new NotImplementedException();
    }

    public IDisposable On<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        throw new NotImplementedException();
    }

    public IDisposable Once<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        throw new NotImplementedException();
    }

    public void Off<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        throw new NotImplementedException();
    }

    public IDisposable On<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }
}
