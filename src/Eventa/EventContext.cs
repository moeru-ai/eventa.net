namespace Eventa;

public sealed class EventContext(IEventaAdapter? adapter = null) : IEventContext
{
    private readonly object _sync = new();
    private readonly Dictionary<string, HashSet<Delegate>> _listeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<Delegate>> _onceListeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MatchListenerRegistration> _matchListeners = new(StringComparer.Ordinal);

    public IDictionary<string, object> Extensions { get; } = new Dictionary<string, object>();

    public IEventaAdapter? Adapter { get; } = adapter;

    public static IEventContext Create(IEventaAdapter? adapter = null)
    {
        return new EventContext(adapter);
    }

    public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
    {
        EmitCore(eventDefinition, payload, options: null);
    }

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
                if (!registration.Matcher(envelope))
                {
                    continue;
                }

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

    public IDisposable On<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            if (!_listeners.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                registeredListeners = new HashSet<Delegate>();
                _listeners[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(handler);
        }

        return new ActionDisposable(() => RemoveListener(eventDefinition.Id, handler, removeOnceListeners: false));
    }

    public IDisposable Once<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            if (!_onceListeners.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                registeredListeners = new HashSet<Delegate>();
                _onceListeners[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(handler);
        }

        return new ActionDisposable(() => RemoveListener(eventDefinition.Id, handler, removeOnceListeners: true));
    }

    public void Off<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);

        lock (_sync)
        {
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

    public IDisposable On<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        ArgumentNullException.ThrowIfNull(matchExpression);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
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

    public void Dispose()
    {
        lock (_sync)
        {
            _listeners.Clear();
            _onceListeners.Clear();
            _matchListeners.Clear();
        }

        Adapter?.Dispose();
    }

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

    private void RemoveMatchListener(string matchExpressionId, Delegate handler)
    {
        lock (_sync)
        {
            if (!_matchListeners.TryGetValue(matchExpressionId, out var registration))
            {
                return;
            }

            registration.Listeners.Remove(handler);
            if (registration.Listeners.Count == 0)
            {
                _matchListeners.Remove(matchExpressionId);
            }
        }
    }

    private static void RemoveListenerCore(
        string eventId,
        Delegate handler,
        IDictionary<string, HashSet<Delegate>> registry)
    {
        if (!registry.TryGetValue(eventId, out var listeners))
        {
            return;
        }

        listeners.Remove(handler);
        if (listeners.Count == 0)
        {
            registry.Remove(eventId);
        }
    }

    private sealed class MatchListenerRegistration(string id, Func<object, bool> matcher)
    {
        public string Id { get; } = id;

        public Func<object, bool> Matcher { get; } = matcher;

        public HashSet<Delegate> Listeners { get; } = [];
    }
}
