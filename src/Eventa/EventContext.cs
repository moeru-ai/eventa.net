using System.Runtime.CompilerServices;

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
public sealed class EventContext(IEventaAdapter? adapter = null) : IEventContext
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, HashSet<Delegate>> _listeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<Delegate>> _onceListeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Type> _eventPayloadTypes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MatchListenerRegistration> _matchListeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Type> _matchExpressionPayloadTypes = new(StringComparer.Ordinal);

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
        ArgumentNullException.ThrowIfNull(eventDefinition);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition);
            BindEventPayloadType(eventDefinition);
        }

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
        ArgumentNullException.ThrowIfNull(eventDefinition);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition);
            BindEventPayloadType(eventDefinition);
        }

        EmitCore(eventDefinition, payload, options);
    }

    private void EmitCore<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        object? options)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);

        var envelope = new EventEnvelope<TPayload>(eventDefinition.Id, payload);
        var listeners = new List<Action<EventEnvelope<TPayload>>>();
        var onceListeners = new List<Action<EventEnvelope<TPayload>>>();
        var matchedListeners = new List<(string MatchExpressionId, Action<EventEnvelope<TPayload>> Handler)>();

        lock (_sync)
        {
            if (_listeners.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                listeners.AddRange(registeredListeners.Cast<Action<EventEnvelope<TPayload>>>());
            }

            if (_onceListeners.TryGetValue(eventDefinition.Id, out var registeredOnceListeners))
            {
                onceListeners.AddRange(registeredOnceListeners.Cast<Action<EventEnvelope<TPayload>>>());
                _onceListeners.Remove(eventDefinition.Id);
            }

            foreach (var registration in _matchListeners.Values)
            {
                if (!registration.Matcher(envelope)) { continue; }

                foreach (var handler in registration.Listeners.Cast<Action<EventEnvelope<TPayload>>>())
                {
                    matchedListeners.Add((registration.Id, handler));
                }
            }
        }

        foreach (var handler in listeners)
        {
            handler(envelope);
            Adapter?.OnReceived(eventDefinition.Id, envelope);
        }

        foreach (var handler in onceListeners)
        {
            handler(envelope);
            Adapter?.OnReceived(eventDefinition.Id, envelope);
        }

        foreach (var (matchExpressionId, handler) in matchedListeners)
        {
            handler(envelope);
            Adapter?.OnReceived(matchExpressionId, envelope);
        }

        Adapter?.OnSent(eventDefinition.Id, envelope, options);
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
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition);
            BindEventPayloadType(eventDefinition);

            if (!_listeners.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                registeredListeners = [];
                _listeners[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(handler);
        }

        return new ActionDisposable(() => RemoveListener(eventDefinition.Id, handler, removeOnceListeners: false));
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
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition);
            BindEventPayloadType(eventDefinition);

            if (!_onceListeners.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                registeredListeners = [];
                _onceListeners[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(handler);
        }

        return new ActionDisposable(() => RemoveListener(eventDefinition.Id, handler, removeOnceListeners: true));
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
        ArgumentNullException.ThrowIfNull(eventDefinition);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition);

            if (handler is null)
            {
                _listeners.Remove(eventDefinition.Id);
                _onceListeners.Remove(eventDefinition.Id);
                return;
            }

            RemoveListenerCore(eventDefinition.Id, handler, _listeners);
            RemoveListenerCore(eventDefinition.Id, handler, _onceListeners);
        }
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
        ArgumentNullException.ThrowIfNull(matchExpression);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            CheckMatchExpressionPayloadTypeBinding(matchExpression);
            BindMatchExpressionPayloadType(matchExpression);

            if (!_matchListeners.TryGetValue(matchExpression.Id, out var registration))
            {
                registration = new MatchListenerRegistration(
                    matchExpression.Id,
                    envelope => envelope is EventEnvelope<TPayload> typedEnvelope && matchExpression.Matcher(typedEnvelope));
                _matchListeners[matchExpression.Id] = registration;
            }

            registration.Listeners.Add(handler);
        }

        return new ActionDisposable(() => RemoveMatchListener(matchExpression.Id, handler));
    }

    /// <summary>
    /// Removes every listener and payload-type binding owned by this context, then disposes the adapter.
    /// </summary>
    public void Dispose()
    {
        lock (_sync)
        {
            _listeners.Clear();
            _onceListeners.Clear();
            _eventPayloadTypes.Clear();
            _matchListeners.Clear();
            _matchExpressionPayloadTypes.Clear();
        }

        Adapter?.Dispose();
    }

    /// <summary>
    /// Removes one direct listener from either the regular or one-shot registry.
    /// </summary>
    /// <param name="eventId">The event identifier that owns the listener.</param>
    /// <param name="handler">The delegate instance to remove.</param>
    /// <param name="removeOnceListeners">
    /// <see langword="true"/> to remove from one-shot listeners; otherwise remove from regular listeners.
    /// </param>
    private void RemoveListener(string eventId, Delegate handler, bool removeOnceListeners)
    {
        lock (_sync)
        {
            if (removeOnceListeners)
            {
                RemoveListenerCore(eventId, handler, _onceListeners);
                return;
            }

            RemoveListenerCore(eventId, handler, _listeners);
        }
    }

    /// <summary>
    /// Removes one listener from a match-expression registration and drops the registration when empty.
    /// </summary>
    /// <param name="matchExpressionId">The match expression identifier that owns the listener.</param>
    /// <param name="handler">The delegate instance to remove.</param>
    private void RemoveMatchListener(string matchExpressionId, Delegate handler)
    {
        lock (_sync)
        {
            if (!_matchListeners.TryGetValue(matchExpressionId, out var registration)) return;

            registration.Listeners.Remove(handler);
            if (registration.Listeners.Count == 0)
            {
                _matchListeners.Remove(matchExpressionId);
            }
        }
    }

    /// <summary>
    /// Removes one delegate from the supplied listener registry and deletes empty buckets.
    /// </summary>
    /// <param name="eventId">The event identifier that keys the registry bucket.</param>
    /// <param name="handler">The delegate instance to remove.</param>
    /// <param name="registry">The listener registry to mutate.</param>
    private static void RemoveListenerCore(
        string eventId,
        Delegate handler,
        IDictionary<string, HashSet<Delegate>> registry)
    {
        if (!registry.TryGetValue(eventId, out var listeners)) return;

        listeners.Remove(handler);
        if (listeners.Count == 0)
        {
            registry.Remove(eventId);
        }
    }

    /// <summary>
    /// Records the payload type associated with an event identifier once it is first observed.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose identifier should be bound.</param>
    private void BindEventPayloadType<TPayload>(EventDefinition<TPayload> eventDefinition)
    {
        BindPayloadTypeCore(_eventPayloadTypes, eventDefinition.Id, typeof(TPayload));
    }

    /// <summary>
    /// Records the payload type associated with a match expression identifier once it is first observed.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose identifier should be bound.</param>
    private void BindMatchExpressionPayloadType<TPayload>(MatchExpression<TPayload> matchExpression)
    {
        BindPayloadTypeCore(_matchExpressionPayloadTypes, matchExpression.Id, typeof(TPayload));
    }


    /// <summary>
    /// Stores the first payload-type binding for an identifier and ignores later writes.
    /// </summary>
    /// <param name="registry">The identifier-to-payload-type registry to update.</param>
    /// <param name="id">The event or match-expression identifier being bound.</param>
    /// <param name="payloadType">The payload type associated with the identifier.</param>
    private static void BindPayloadTypeCore(
        IDictionary<string, Type> registry,
        string id,
        Type payloadType)
    {
        if (!registry.ContainsKey(id))
        {
            registry[id] = payloadType;
        }
    }

    /// <summary>
    /// Verifies that an event identifier has not already been bound to a different payload type.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose binding should be checked.</param>
    /// <param name="operation">The calling operation used in the exception message.</param>
    private void CheckEventPayloadTypeBinding<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        [CallerMemberName] string operation = "")
    {
        CheckPayloadTypeBindingCore(
            _eventPayloadTypes,
            eventDefinition,
            eventDefinition.Id,
            typeof(TPayload),
            operation);
    }


    /// <summary>
    /// Verifies that a match expression identifier has not already been bound to a different payload type.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose binding should be checked.</param>
    /// <param name="operation">The calling operation used in the exception message.</param>
    private void CheckMatchExpressionPayloadTypeBinding<TPayload>(
        MatchExpression<TPayload> matchExpression,
        [CallerMemberName] string operation = "")
    {
        CheckPayloadTypeBindingCore(
            _matchExpressionPayloadTypes,
            matchExpression,
            matchExpression.Id,
            typeof(TPayload),
            operation);
    }

    /// <summary>
    /// Throws when an identifier is already bound to a conflicting payload type.
    /// </summary>
    /// <typeparam name="TBinding">The binding object type used to describe the failing target.</typeparam>
    /// <param name="registry">The identifier-to-payload-type registry to inspect.</param>
    /// <param name="binding">The binding instance used to describe the failing target in the message.</param>
    /// <param name="id">The identifier being checked.</param>
    /// <param name="currentType">The payload type requested by the current operation.</param>
    /// <param name="operation">The calling operation used in the exception message.</param>
    private static void CheckPayloadTypeBindingCore<TBinding>(
        IDictionary<string, Type> registry,
        TBinding binding,
        string id,
        Type currentType,
        string operation)
    {
        if (!registry.TryGetValue(id, out var boundType) || boundType == currentType) return;

        var bindingTarget = DescribeBindingTarget(binding);

        throw new InvalidOperationException(
            $"Cannot perform '{operation}' for {bindingTarget} '{id}' with payload type '{FormatTypeName(currentType)}' " +
            $"because this EventContext already bound {bindingTarget} '{id}' to payload type '{FormatTypeName(boundType)}'.");
    }

    /// <summary>
    /// Formats the binding type name used in payload-conflict exception messages.
    /// </summary>
    /// <typeparam name="TBinding">The binding object type being described.</typeparam>
    /// <param name="binding">The binding instance whose logical type should be reported.</param>
    /// <returns>The normalized binding type name used in diagnostics.</returns>
    private static string DescribeBindingTarget<TBinding>(TBinding binding)
    {
        var bindingType = binding?.GetType() ?? typeof(TBinding);
        var genericDefinition = bindingType.IsGenericType
            ? bindingType.GetGenericTypeDefinition()
            : bindingType;

        if (genericDefinition == typeof(EventDefinition<>))
        {
            return nameof(EventDefinition<>);
        }

        if (genericDefinition == typeof(MatchExpression<>))
        {
            return nameof(MatchExpression<>);
        }

        return genericDefinition.Name;
    }

    /// <summary>
    /// Formats a payload type for conflict diagnostics.
    /// </summary>
    /// <param name="type">The runtime type to format.</param>
    /// <returns>The formatted type name.</returns>
    private static string FormatTypeName(Type type)
    {
        return type.ToString();
    }

    /// <summary>
    /// Holds one match expression's compiled matcher and subscribed delegates inside the context.
    /// </summary>
    /// <param name="id">The stable identifier for the match expression registration.</param>
    /// <param name="matcher">The runtime matcher used to test emitted envelopes.</param>
    private sealed class MatchListenerRegistration(string id, Func<object, bool> matcher)
    {
        /// <summary>
        /// Gets the match expression identifier associated with this registration.
        /// </summary>
        public string Id { get; } = id;

        /// <summary>
        /// Gets the matcher that decides whether an emitted envelope should notify these listeners.
        /// </summary>
        public Func<object, bool> Matcher { get; } = matcher;

        /// <summary>
        /// Gets the listeners currently subscribed to this match expression.
        /// </summary>
        public HashSet<Delegate> Listeners { get; } = [];
    }
}
