namespace Eventa;

/// <summary>
/// Provides Eventa's default in-memory implementation of <see cref="IEventContext"/>.
/// </summary>
/// <remarks>
/// <para>
/// The context dispatches event listeners synchronously on the caller thread and keeps a
/// per-context binding between each event or match-expression identifier and exactly one payload
/// type.
/// </para>
/// <para>
/// Adapter hooks are notified after local listeners run, which lets transports observe sent and
/// received envelopes without taking over the in-process dispatch path.
/// </para>
/// </remarks>
public sealed class EventContext(IEventaAdapter? adapter = null) :
    IEventContext,
    IEventInboundDispatcher,
    IEventTransportFatalNotifier
{
    private readonly EventListenerStore _listeners = new();

    /// <summary>
    /// Gets the mutable extension bag associated with this context.
    /// </summary>
    /// <remarks>
    /// Eventa uses this bag for per-context features such as invoke abort registrations, and
    /// adapters can use it to attach transport-specific state.
    /// </remarks>
    public IDictionary<string, object> Extensions { get; } = new Dictionary<string, object>();

    /// <summary>
    /// Gets the adapter that observes local send and receive activity for this context.
    /// </summary>
    public IEventaAdapter? Adapter { get; } = adapter;

    /// <summary>
    /// Creates a new in-memory event context.
    /// </summary>
    /// <param name="adapter">
    /// An optional adapter that should observe emitted and received envelopes after local dispatch.
    /// </param>
    /// <returns>A new <see cref="EventContext"/> instance.</returns>
    public static IEventContext Create(IEventaAdapter? adapter = null)
    {
        return new EventContext(adapter);
    }

    /// <summary>
    /// Emits an event to all direct and match-expression listeners registered for its identifier.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition that identifies the channel to emit on.</param>
    /// <param name="payload">The payload value to wrap in the emitted envelope.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="eventDefinition"/> with a
    /// different payload type.
    /// </exception>
    public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
    {
        EmitCore(eventDefinition, payload, options: null);
    }

    /// <summary>
    /// Emits an event and forwards adapter-specific metadata to <see cref="IEventaAdapter.OnSent"/>.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <typeparam name="TOptions">The reference-type metadata forwarded to the adapter.</typeparam>
    /// <param name="eventDefinition">The event definition that identifies the channel to emit on.</param>
    /// <param name="payload">The payload value to wrap in the emitted envelope.</param>
    /// <param name="options">Optional adapter metadata associated with this emit operation.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="eventDefinition"/> with a
    /// different payload type.
    /// </exception>
    public void Emit<TPayload, TOptions>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        TOptions options)
        where TOptions : class
    {
        EmitCore(eventDefinition, payload, options);
    }

    private void EmitCore<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        object? options)
    {
        var dispatch = _listeners.CreateDispatchSnapshot(eventDefinition, payload, nameof(Emit));

        foreach (var handler in dispatch.Listeners)
        {
            handler(dispatch.Envelope);
            Adapter?.OnReceived(eventDefinition.Id, dispatch.Envelope);
        }

        foreach (var handler in dispatch.OnceListeners)
        {
            handler(dispatch.Envelope);
            Adapter?.OnReceived(eventDefinition.Id, dispatch.Envelope);
        }

        foreach (var matchedListener in dispatch.MatchedListeners)
        {
            matchedListener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(matchedListener.MatchExpressionId, dispatch.Envelope);
        }

        foreach (var matchedListener in dispatch.MatchedOnceListeners)
        {
            matchedListener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(matchedListener.MatchExpressionId, dispatch.Envelope);
        }

        Adapter?.OnSent(eventDefinition.Id, dispatch.Envelope, options);
    }

    /// <summary>
    /// Dispatches a transport-originated envelope locally without notifying <see cref="IEventaAdapter.OnSent"/>.
    /// </summary>
    /// <param name="envelope">The already-created envelope to dispatch.</param>
    /// <param name="options">Optional adapter metadata forwarded to <see cref="IEventaAdapter.OnReceived(string, object?, object?)"/>.</param>
    public void Receive(IEventEnvelope envelope, object? options = null)
    {
        var dispatch = _listeners.CreateDispatchSnapshot(envelope, nameof(Receive));

        foreach (var listener in dispatch.Listeners)
        {
            listener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(listener.EventId, dispatch.Envelope, options);
        }

        foreach (var listener in dispatch.OnceListeners)
        {
            listener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(listener.EventId, dispatch.Envelope, options);
        }

        foreach (var listener in dispatch.MatchedListeners)
        {
            listener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(listener.EventId, dispatch.Envelope, options);
        }

        foreach (var listener in dispatch.MatchedOnceListeners)
        {
            listener.Handler(dispatch.Envelope);
            Adapter?.OnReceived(listener.EventId, dispatch.Envelope, options);
        }
    }

    /// <summary>
    /// Faults invoke sessions that are pending on this context because its transport terminated.
    /// </summary>
    /// <param name="error">The terminal transport error.</param>
    public void NotifyTransportFatal(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        if (this.TryGetFeature<InvokeInternalConfig>(InvokeExtensions.InternalInvokeConfigKey, out var internalConfig))
        {
            internalConfig.TransportFatalInvocations.Notify(error);
        }
    }

    /// <summary>
    /// Registers a listener for every future emission of the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose emissions should be observed.</param>
    /// <param name="handler">The callback that receives the emitted envelope.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this listener from the context when disposed.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="eventDefinition"/> with a
    /// different payload type.
    /// </exception>
    public IDisposable Subscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        return _listeners.Subscribe(eventDefinition, handler, EventListenerLifetime.Regular, nameof(Subscribe));
    }

    /// <summary>
    /// Registers a listener that runs at most once for the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose next emission should be observed.</param>
    /// <param name="handler">The callback that receives the emitted envelope.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this pending one-shot listener when disposed.
    /// </returns>
    /// <remarks>
    /// The listener is removed before callbacks are invoked, so re-entrant emits do not run the
    /// same one-shot handler twice.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="eventDefinition"/> with a
    /// different payload type.
    /// </exception>
    public IDisposable SubscribeOnce<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        return _listeners.Subscribe(eventDefinition, handler, EventListenerLifetime.Once, nameof(SubscribeOnce));
    }

    /// <summary>
    /// Removes one listener or all listeners associated with the specified event.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose listeners should be removed.</param>
    /// <param name="handler">
    /// The specific listener to remove. When <see langword="null"/>, all regular and one-shot
    /// listeners for the event are removed.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="eventDefinition"/> with a
    /// different payload type.
    /// </exception>
    public void Unsubscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        _listeners.Unsubscribe(eventDefinition, handler, nameof(Unsubscribe));
    }

    /// <summary>
    /// Registers a listener for all emitted events whose envelopes satisfy a match expression.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The reusable matcher that selects envelopes to observe.</param>
    /// <param name="handler">The callback that receives envelopes matching the expression.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this match listener when disposed.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="matchExpression"/> with a
    /// different payload type.
    /// </exception>
    public IDisposable Subscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        return _listeners.Subscribe(matchExpression, handler, EventListenerLifetime.Regular, nameof(Subscribe));
    }

    /// <summary>
    /// Registers a listener that runs at most once for emitted events matching the expression.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The reusable matcher that selects envelopes to observe.</param>
    /// <param name="handler">The callback that receives the first envelope matching the expression.</param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes this pending one-shot match listener when disposed.
    /// </returns>
    /// <remarks>
    /// The listener is removed before callbacks are invoked, so re-entrant emits do not run the
    /// same one-shot match handler twice.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="matchExpression"/> with a
    /// different payload type.
    /// </exception>
    public IDisposable SubscribeOnce<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        return _listeners.Subscribe(matchExpression, handler, EventListenerLifetime.Once, nameof(SubscribeOnce));
    }

    /// <summary>
    /// Removes one listener or all listeners associated with the specified match expression.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The reusable matcher whose listeners should be removed.</param>
    /// <param name="handler">
    /// The specific listener to remove. When <see langword="null"/>, all regular and one-shot
    /// listeners for the match expression are removed.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this context has already associated <paramref name="matchExpression"/> with a
    /// different payload type.
    /// </exception>
    public void Unsubscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        _listeners.Unsubscribe(matchExpression, handler, nameof(Unsubscribe));
    }

    /// <summary>
    /// Removes every listener and payload-type binding owned by this context, then disposes the adapter.
    /// </summary>
    public void Dispose()
    {
        _listeners.Clear();

        Adapter?.Dispose();
    }
}
