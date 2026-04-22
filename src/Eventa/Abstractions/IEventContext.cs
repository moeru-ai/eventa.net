namespace Eventa;

/// <summary>
/// Coordinates event emission and listener registration for a single event channel.
/// </summary>
/// <remarks>
/// <para>
/// The default <see cref="EventContext"/> implementation binds each
/// <see cref="EventDefinition{TPayload}.Id"/> and <see cref="MatchExpression{TPayload}.Id"/>
/// to a single payload type for the lifetime of the context.
/// </para>
/// <para>
/// Custom <see cref="IEventContext"/> implementations should preserve an equivalent contract so
/// the same identifier is not reused with conflicting payload shapes inside one context.
/// </para>
/// </remarks>
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
