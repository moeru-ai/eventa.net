namespace Eventa;

/// <summary>
/// Observes local Eventa send and receive activity so transports can mirror or instrument it.
/// </summary>
/// <remarks>
/// Implementations are called after in-process dispatch has already happened, so adapter code can
/// forward envelopes or record telemetry without taking control of listener execution order.
/// </remarks>
public interface IEventaAdapter : IDisposable
{
    /// <summary>
    /// Called after <see cref="IEventContext.Emit{TPayload}(EventDefinition{TPayload}, TPayload)"/> finishes local dispatch.
    /// The runtime value is the emitted <see cref="EventEnvelope{TPayload}"/>, exposed as <see cref="object"/> because
    /// the adapter interface is non-generic.
    /// </summary>
    /// <param name="eventId">The emitted event identifier.</param>
    /// <param name="envelope">The emitted <see cref="EventEnvelope{TPayload}"/> instance.</param>
    /// <param name="options">Optional emit metadata forwarded from <see cref="IEventContext.Emit{TPayload, TOptions}(EventDefinition{TPayload}, TPayload, TOptions)"/>.</param>
    void OnSent(string eventId, object? envelope, object? options = null);

    /// <summary>
    /// Called after local listener dispatch completes for <see cref="IEventContext.Emit{TPayload}(EventDefinition{TPayload}, TPayload)"/>,
    /// or after <see cref="IEventInboundDispatcher.Receive(IEventEnvelope, object?)"/> dispatches a transport-originated envelope.
    /// </summary>
    /// <param name="eventId">
    /// The listener key that received the event. This is the original event id for direct subscriptions, and can be a
    /// match-expression id for match listeners.
    /// </param>
    /// <param name="envelope">
    /// The received envelope, exposed as <see cref="object"/> because the adapter interface is non-generic.
    /// For local emit notifications (<paramref name="options"/> is <see langword="null"/>), the runtime value is the
    /// emitted <see cref="EventEnvelope{TPayload}"/> instance. For transport-originated notifications, the runtime value
    /// can be any <see cref="IEventEnvelope"/> implementation passed to <see cref="IEventInboundDispatcher.Receive(IEventEnvelope, object?)"/>.
    /// When <paramref name="eventId"/> is a match-expression id, inspect <see cref="IEventEnvelope.EventId"/> on the
    /// runtime envelope to get the original event id.
    /// </param>
    /// <param name="options">
    /// Optional metadata forwarded from <see cref="IEventInboundDispatcher.Receive(IEventEnvelope, object?)"/>, or
    /// <see langword="null"/> for local emit notifications.
    /// </param>
    void OnReceived(string eventId, object? envelope, object? options = null);
}
