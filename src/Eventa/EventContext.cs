using System.Runtime.CompilerServices;

namespace Eventa;

public sealed class EventContext(IEventaAdapter? adapter = null) : IEventContext
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, HashSet<Delegate>> _listeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<Delegate>> _onceListeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Type> _eventPayloadTypes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MatchListenerRegistration> _matchListeners = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Type> _matchExpressionPayloadTypes = new(StringComparer.Ordinal);

    public IDictionary<string, object> Extensions { get; } = new Dictionary<string, object>();

    public IEventaAdapter? Adapter { get; } = adapter;

    public static IEventContext Create(IEventaAdapter? adapter = null)
    {
        return new EventContext(adapter);
    }

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
            if (!_matchListeners.TryGetValue(matchExpressionId, out var registration)) return;

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
        if (!registry.TryGetValue(eventId, out var listeners)) return;

        listeners.Remove(handler);
        if (listeners.Count == 0)
        {
            registry.Remove(eventId);
        }
    }

    private void BindEventPayloadType<TPayload>(EventDefinition<TPayload> eventDefinition)
    {
        BindPayloadTypeCore(_eventPayloadTypes, eventDefinition.Id, typeof(TPayload));
    }

    private void BindMatchExpressionPayloadType<TPayload>(MatchExpression<TPayload> matchExpression)
    {
        BindPayloadTypeCore(_matchExpressionPayloadTypes, matchExpression.Id, typeof(TPayload));
    }


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

    private static string FormatTypeName(Type type)
    {
        return type.ToString();
    }

    private sealed class MatchListenerRegistration(string id, Func<object, bool> matcher)
    {
        public string Id { get; } = id;

        public Func<object, bool> Matcher { get; } = matcher;

        public HashSet<Delegate> Listeners { get; } = [];
    }
}
