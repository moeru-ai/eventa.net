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
    /// <summary>
    /// Gets the mutable extension bag associated with this context.
    /// </summary>
    /// <remarks>
    /// Eventa uses this bag for per-context features such as invoke abort registrations, and
    /// adapters can use it to attach transport-specific state.
    /// </remarks>
    IDictionary<string, object> Extensions { get; }

    /// <summary>
    /// Emits an event to all direct and match-expression listeners registered for its identifier.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition that identifies the channel to emit on.</param>
    /// <param name="payload">The payload value to wrap in the emitted envelope.</param>
    void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload);

    /// <summary>
    /// Emits an event and forwards adapter-specific metadata to <see cref="IEventaAdapter.OnSent"/>.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <typeparam name="TOptions">The reference-type metadata forwarded to the adapter.</typeparam>
    /// <param name="eventDefinition">The event definition that identifies the channel to emit on.</param>
    /// <param name="payload">The payload value to wrap in the emitted envelope.</param>
    /// <param name="options">Optional adapter metadata associated with this emit operation.</param>
    void Emit<TPayload, TOptions>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        TOptions options)
        where TOptions : class;

    /// <summary>
    /// Registers a listener for every future emission of the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose emissions should be observed.</param>
    /// <param name="handler">The callback that receives the emitted envelope.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this listener from the context when disposed.
    /// </returns>
    IDisposable Subscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler);

    /// <summary>
    /// Registers a listener that runs at most once for the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose next emission should be observed.</param>
    /// <param name="handler">The callback that receives the emitted envelope.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this pending one-shot listener when disposed.
    /// </returns>
    IDisposable SubscribeOnce<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler);

    /// <summary>
    /// Removes one listener or all listeners associated with the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose listeners should be removed.</param>
    /// <param name="handler">
    /// The specific listener to remove. When <see langword="null"/>, all regular and one-shot
    /// listeners for the event are removed.
    /// </param>
    void Unsubscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null);

    /// <summary>
    /// Registers a listener for all emitted events whose envelopes satisfy a match expression.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The reusable matcher that selects envelopes to observe.</param>
    /// <param name="handler">The callback that receives envelopes matching the expression.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this match listener when disposed.
    /// </returns>
    IDisposable Subscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler);
}
