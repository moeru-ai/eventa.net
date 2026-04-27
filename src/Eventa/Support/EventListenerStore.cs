using System.Runtime.CompilerServices;

namespace Eventa;

/// <summary>
/// Owns listener storage, once-listener removal, match-expression storage, and payload type bindings.
/// </summary>
internal sealed class EventListenerStore
{
    /// <summary>
    /// Guards all listener registries and payload-type binding maps.
    /// </summary>
    private readonly Lock _sync = new();

    /// <summary>
    /// Stores persistent direct event listeners by event id.
    /// </summary>
    private readonly Dictionary<string, HashSet<Delegate>> _listeners = new(StringComparer.Ordinal);

    /// <summary>
    /// Stores one-shot direct event listeners by event id.
    /// </summary>
    private readonly Dictionary<string, HashSet<Delegate>> _onceListeners = new(StringComparer.Ordinal);

    /// <summary>
    /// Stores the payload type first bound to each event id.
    /// </summary>
    private readonly Dictionary<string, Type> _eventPayloadTypes = new(StringComparer.Ordinal);

    /// <summary>
    /// Stores match-expression registrations by match-expression id.
    /// </summary>
    private readonly Dictionary<string, MatchListenerRegistration> _matchListeners = new(StringComparer.Ordinal);

    /// <summary>
    /// Stores the payload type first bound to each match-expression id.
    /// </summary>
    private readonly Dictionary<string, Type> _matchExpressionPayloadTypes = new(StringComparer.Ordinal);

    /// <summary>
    /// Registers a direct event listener in the selected listener bucket.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose emissions should be observed.</param>
    /// <param name="handler">The callback that receives matching event envelopes.</param>
    /// <param name="lifetime">The listener lifetime bucket to register into.</param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    /// <returns>An <see cref="IDisposable"/> that removes this listener from the selected bucket.</returns>
    public IDisposable Subscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler,
        EventListenerLifetime lifetime,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition, operation);
            BindEventPayloadType(eventDefinition);

            var registry = lifetime switch
            {
                EventListenerLifetime.Regular => _listeners,
                EventListenerLifetime.Once => _onceListeners,
                _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, null),
            };

            if (!registry.TryGetValue(eventDefinition.Id, out var registeredListeners))
            {
                registeredListeners = [];
                registry[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(handler);
        }

        var bucketKind = lifetime switch
        {
            EventListenerLifetime.Regular => DirectListenerRegistryKind.Regular,
            EventListenerLifetime.Once => DirectListenerRegistryKind.Once,
            _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, null),
        };

        return new ActionDisposable(() => RemoveDirectListenerFromRegistry(eventDefinition.Id, handler, bucketKind));
    }

    /// <summary>
    /// Removes one direct event listener or all listeners for an event id.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose listener bucket should be updated.</param>
    /// <param name="handler">
    /// The callback to remove. When <see langword="null"/>, all direct regular and one-shot
    /// listeners for the event are removed.
    /// </param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    public void Unsubscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition, operation);

            if (handler is null)
            {
                _listeners.Remove(eventDefinition.Id);
                _onceListeners.Remove(eventDefinition.Id);
                return;
            }

            RemoveDirectListenerFromRegistry(eventDefinition.Id, handler, DirectListenerRegistryKind.All);
        }
    }

    /// <summary>
    /// Registers a match-expression listener in the selected listener bucket.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose matching emissions should be observed.</param>
    /// <param name="handler">The callback that receives matching event envelopes.</param>
    /// <param name="lifetime">The listener lifetime bucket to register into.</param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    /// <returns>An <see cref="IDisposable"/> that removes this listener from the selected bucket.</returns>
    public IDisposable Subscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler,
        EventListenerLifetime lifetime,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(matchExpression);
        ArgumentNullException.ThrowIfNull(handler);

        lock (_sync)
        {
            CheckMatchExpressionPayloadTypeBinding(matchExpression, operation);
            BindMatchExpressionPayloadType(matchExpression);

            if (!_matchListeners.TryGetValue(matchExpression.Id, out var registration))
            {
                registration = CreateMatchListenerRegistration(matchExpression);
                _matchListeners[matchExpression.Id] = registration;
            }

            var bucket = lifetime switch
            {
                EventListenerLifetime.Regular => registration.Listeners,
                EventListenerLifetime.Once => registration.OnceListeners,
                _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, null),
            };

            bucket.Add(handler);
        }

        var bucketKind = lifetime switch
        {
            EventListenerLifetime.Regular => MatchListenerBucketKind.Regular,
            EventListenerLifetime.Once => MatchListenerBucketKind.Once,
            _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, null),
        };

        return new ActionDisposable(() => RemoveMatchListenerFromBuckets(matchExpression.Id, handler, bucketKind));
    }

    /// <summary>
    /// Removes one match-expression listener or all listeners for a match-expression id.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose listener bucket should be updated.</param>
    /// <param name="handler">
    /// The callback to remove. When <see langword="null"/>, all regular and one-shot listeners
    /// for the match expression are removed.
    /// </param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    public void Unsubscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>>? handler,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(matchExpression);

        lock (_sync)
        {
            CheckMatchExpressionPayloadTypeBinding(matchExpression, operation);

            if (handler is null)
            {
                _matchListeners.Remove(matchExpression.Id);
                return;
            }

            RemoveMatchListenerFromBuckets(matchExpression.Id, handler, MatchListenerBucketKind.All);
        }
    }

    /// <summary>
    /// Creates a dispatch snapshot and removes any one-shot listeners before callbacks run.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the emitted event.</typeparam>
    /// <param name="eventDefinition">The event definition being emitted.</param>
    /// <param name="payload">The payload to wrap in the emitted envelope.</param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    /// <returns>
    /// A stable listener snapshot that can be invoked outside the store lock.
    /// </returns>
    public EventDispatchSnapshot<TPayload> CreateDispatchSnapshot<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(eventDefinition);

        var envelope = new EventEnvelope<TPayload>(eventDefinition.Id, payload);
        var listeners = new List<Action<EventEnvelope<TPayload>>>();
        var onceListeners = new List<Action<EventEnvelope<TPayload>>>();
        var matchedListeners = new List<MatchListenerDispatch<TPayload>>();
        var matchedOnceListeners = new List<MatchListenerDispatch<TPayload>>();
        var emptyMatchRegistrations = new List<string>();

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition, operation);
            BindEventPayloadType(eventDefinition);

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
                    matchedListeners.Add(new MatchListenerDispatch<TPayload>(registration.Id, handler));
                }

                foreach (var handler in registration.OnceListeners.Cast<Action<EventEnvelope<TPayload>>>())
                {
                    matchedOnceListeners.Add(new MatchListenerDispatch<TPayload>(registration.Id, handler));
                }

                registration.OnceListeners.Clear();
                if (registration.IsEmpty)
                {
                    emptyMatchRegistrations.Add(registration.Id);
                }
            }

            foreach (var matchExpressionId in emptyMatchRegistrations)
            {
                _matchListeners.Remove(matchExpressionId);
            }
        }

        return new EventDispatchSnapshot<TPayload>(
            envelope,
            listeners,
            onceListeners,
            matchedListeners,
            matchedOnceListeners);
    }

    /// <summary>
    /// Removes every listener and payload-type binding.
    /// </summary>
    public void Clear()
    {
        lock (_sync)
        {
            _listeners.Clear();
            _onceListeners.Clear();
            _eventPayloadTypes.Clear();
            _matchListeners.Clear();
            _matchExpressionPayloadTypes.Clear();
        }
    }

    /// <summary>
    /// Removes a direct listener from one or both direct listener registries.
    /// </summary>
    /// <param name="eventId">The event id whose listener bucket should be updated.</param>
    /// <param name="handler">The delegate instance to remove.</param>
    /// <param name="registryKind">The direct listener bucket or buckets to inspect.</param>
    private void RemoveDirectListenerFromRegistry(
        string eventId,
        Delegate handler,
        DirectListenerRegistryKind registryKind)
    {
        lock (_sync)
        {
            void RemoveFrom(Dictionary<string, HashSet<Delegate>> registry)
            {
                if (!registry.TryGetValue(eventId, out var listeners)) return;

                listeners.Remove(handler);
                if (listeners.Count == 0)
                {
                    registry.Remove(eventId);
                }
            }

            var (removeRegular, removeOnce) = registryKind switch
            {
                DirectListenerRegistryKind.Regular => (true, false),
                DirectListenerRegistryKind.Once => (false, true),
                DirectListenerRegistryKind.All => (true, true),
                _ => throw new ArgumentOutOfRangeException(nameof(registryKind), registryKind, null),
            };

            if (removeRegular)
            {
                RemoveFrom(_listeners);
            }

            if (removeOnce)
            {
                RemoveFrom(_onceListeners);
            }
        }
    }

    /// <summary>
    /// Removes a match-expression listener from one or both match listener buckets.
    /// </summary>
    /// <param name="matchExpressionId">The match-expression id whose listener bucket should be updated.</param>
    /// <param name="handler">The delegate instance to remove.</param>
    /// <param name="bucketKind">The match listener bucket or buckets to inspect.</param>
    private void RemoveMatchListenerFromBuckets(
        string matchExpressionId,
        Delegate handler,
        MatchListenerBucketKind bucketKind)
    {
        lock (_sync)
        {
            if (!_matchListeners.TryGetValue(matchExpressionId, out var registration)) return;

            var (removeRegular, removeOnce) = bucketKind switch
            {
                MatchListenerBucketKind.Regular => (true, false),
                MatchListenerBucketKind.Once => (false, true),
                MatchListenerBucketKind.All => (true, true),
                _ => throw new ArgumentOutOfRangeException(nameof(bucketKind), bucketKind, null),
            };

            if (removeRegular)
            {
                registration.Listeners.Remove(handler);
            }

            if (removeOnce)
            {
                registration.OnceListeners.Remove(handler);
            }

            if (registration.IsEmpty)
            {
                _matchListeners.Remove(matchExpressionId);
            }
        }
    }

    /// <summary>
    /// Selects the direct event listener registry to mutate.
    /// </summary>
    private enum DirectListenerRegistryKind
    {
        /// <summary>
        /// Mutate only persistent event listeners.
        /// </summary>
        Regular,

        /// <summary>
        /// Mutate only one-shot event listeners.
        /// </summary>
        Once,

        /// <summary>
        /// Mutate both persistent and one-shot event listeners.
        /// </summary>
        All,
    }

    /// <summary>
    /// Selects the match-expression listener bucket to mutate.
    /// </summary>
    private enum MatchListenerBucketKind
    {
        /// <summary>
        /// Mutate only persistent match-expression listeners.
        /// </summary>
        Regular,

        /// <summary>
        /// Mutate only one-shot match-expression listeners.
        /// </summary>
        Once,

        /// <summary>
        /// Mutate both persistent and one-shot match-expression listeners.
        /// </summary>
        All,
    }

    /// <summary>
    /// Records the payload type associated with an event id on first use.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose id should be bound.</param>
    private void BindEventPayloadType<TPayload>(EventDefinition<TPayload> eventDefinition)
    {
        BindPayloadTypeCore(_eventPayloadTypes, eventDefinition.Id, typeof(TPayload));
    }

    /// <summary>
    /// Records the payload type associated with a match-expression id on first use.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose id should be bound.</param>
    private void BindMatchExpressionPayloadType<TPayload>(MatchExpression<TPayload> matchExpression)
    {
        BindPayloadTypeCore(_matchExpressionPayloadTypes, matchExpression.Id, typeof(TPayload));
    }

    /// <summary>
    /// Stores the first payload-type binding for an identifier and ignores later matching writes.
    /// </summary>
    /// <param name="registry">The identifier-to-payload-type map to update.</param>
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
    /// Verifies that an event id is not already bound to another payload type.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose binding should be checked.</param>
    /// <param name="operation">The caller operation name used in the exception message.</param>
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
    /// Verifies that a match-expression id is not already bound to another payload type.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose binding should be checked.</param>
    /// <param name="operation">The caller operation name used in the exception message.</param>
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
    /// Creates an erased match-listener registration from a typed match expression.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression used to select emitted envelopes.</param>
    /// <returns>A registration that can evaluate emitted envelopes without exposing generic state.</returns>
    private static MatchListenerRegistration CreateMatchListenerRegistration<TPayload>(
        MatchExpression<TPayload> matchExpression)
    {
        return new MatchListenerRegistration(
            matchExpression.Id,
            envelope => envelope is EventEnvelope<TPayload> typedEnvelope && matchExpression.Matcher(typedEnvelope));
    }

    /// <summary>
    /// Throws when an identifier has already been bound to a conflicting payload type.
    /// </summary>
    /// <typeparam name="TBinding">The logical binding object type used for diagnostics.</typeparam>
    /// <param name="registry">The identifier-to-payload-type map to inspect.</param>
    /// <param name="binding">The binding object used to describe the failing target.</param>
    /// <param name="id">The identifier whose payload type is being checked.</param>
    /// <param name="currentType">The payload type requested by the current operation.</param>
    /// <param name="operation">The caller operation name used in the exception message.</param>
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
    /// Formats a binding object as the public logical target name used in diagnostics.
    /// </summary>
    /// <typeparam name="TBinding">The binding object type being described.</typeparam>
    /// <param name="binding">The binding instance whose logical target should be reported.</param>
    /// <returns>A normalized target name such as <c>EventDefinition&lt;&gt;</c>.</returns>
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
    /// Formats a payload type for payload-binding diagnostics.
    /// </summary>
    /// <param name="type">The type to format.</param>
    /// <returns>The formatted type name.</returns>
    private static string FormatTypeName(Type type)
    {
        return type.ToString();
    }

    /// <summary>
    /// Stores one match expression's erased matcher and subscribed delegates.
    /// </summary>
    /// <param name="id">The match-expression id associated with this registration.</param>
    /// <param name="matcher">The erased matcher used to test emitted envelopes.</param>
    private sealed class MatchListenerRegistration(string id, Func<object, bool> matcher)
    {
        /// <summary>
        /// Gets the match-expression id associated with this registration.
        /// </summary>
        public string Id { get; } = id;

        /// <summary>
        /// Gets the erased matcher used to test emitted envelopes.
        /// </summary>
        public Func<object, bool> Matcher { get; } = matcher;

        /// <summary>
        /// Gets persistent match-expression listeners.
        /// </summary>
        public HashSet<Delegate> Listeners { get; } = [];

        /// <summary>
        /// Gets one-shot match-expression listeners.
        /// </summary>
        public HashSet<Delegate> OnceListeners { get; } = [];

        /// <summary>
        /// Gets whether this registration has no remaining listeners.
        /// </summary>
        public bool IsEmpty => Listeners.Count == 0 && OnceListeners.Count == 0;
    }
}

/// <summary>
/// Describes whether a listener should persist or be removed after one dispatch.
/// </summary>
internal enum EventListenerLifetime
{
    /// <summary>
    /// The listener remains registered until explicitly removed.
    /// </summary>
    Regular,

    /// <summary>
    /// The listener is removed before its first callback is invoked.
    /// </summary>
    Once,
}

/// <summary>
/// Contains the listeners selected for one event emission.
/// </summary>
/// <typeparam name="TPayload">The payload type carried by the emitted event.</typeparam>
/// <param name="Envelope">The emitted event envelope shared by every selected listener.</param>
/// <param name="Listeners">Persistent direct event listeners selected by event id.</param>
/// <param name="OnceListeners">One-shot direct event listeners selected by event id.</param>
/// <param name="MatchedListeners">Persistent match-expression listeners selected by matcher.</param>
/// <param name="MatchedOnceListeners">One-shot match-expression listeners selected by matcher.</param>
internal sealed record EventDispatchSnapshot<TPayload>(
    EventEnvelope<TPayload> Envelope,
    IReadOnlyList<Action<EventEnvelope<TPayload>>> Listeners,
    IReadOnlyList<Action<EventEnvelope<TPayload>>> OnceListeners,
    IReadOnlyList<MatchListenerDispatch<TPayload>> MatchedListeners,
    IReadOnlyList<MatchListenerDispatch<TPayload>> MatchedOnceListeners);

/// <summary>
/// Describes a match-expression listener selected for dispatch.
/// </summary>
/// <typeparam name="TPayload">The payload type carried by the emitted event.</typeparam>
/// <param name="MatchExpressionId">The match-expression id that selected the listener.</param>
/// <param name="Handler">The callback to invoke with the emitted envelope.</param>
internal readonly record struct MatchListenerDispatch<TPayload>(
    string MatchExpressionId,
    Action<EventEnvelope<TPayload>> Handler);
