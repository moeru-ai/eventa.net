namespace Eventa;

public interface IEventContext : IDisposable
{
    IDictionary<string, object> Extensions { get; }

    void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload);

    void Emit<TPayload, TOptions>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        TOptions options)
        where TOptions : class;

    IDisposable On<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler);

    IDisposable Once<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler);

    void Off<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null);

    IDisposable On<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler);
}
