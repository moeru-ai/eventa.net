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
    private readonly Dictionary<string, HashSet<EventListenerRegistration>> _listeners = new(StringComparer.Ordinal);

    /// <summary>
    /// Stores one-shot direct event listeners by event id.
    /// </summary>
    private readonly Dictionary<string, HashSet<EventListenerRegistration>> _onceListeners = new(StringComparer.Ordinal);

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
                registeredListeners = new(EventListenerRegistration.HandlerComparer);
                registry[eventDefinition.Id] = registeredListeners;
            }

            registeredListeners.Add(EventListenerRegistration.Create(handler));
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
                registration = MatchListenerRegistration.Create(matchExpression);
                _matchListeners[matchExpression.Id] = registration;
            }

            var bucket = lifetime switch
            {
                EventListenerLifetime.Regular => registration.Listeners,
                EventListenerLifetime.Once => registration.OnceListeners,
                _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, null),
            };

            bucket.Add(EventListenerRegistration.Create(handler));
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
        SelectedListeners selectedListeners;

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(eventDefinition, operation);
            BindEventPayloadType(eventDefinition);
            selectedListeners = CollectSelectedListeners(envelope);
        }

        return selectedListeners.CreateTypedSnapshot(envelope);
    }

    /// <summary>
    /// Creates a dispatch snapshot for a transport-originated envelope and removes one-shot listeners
    /// before callbacks run.
    /// </summary>
    /// <param name="envelope">The already-created envelope to dispatch.</param>
    /// <param name="operation">The caller operation name used in payload-binding diagnostics.</param>
    /// <returns>A stable erased listener snapshot that can be invoked outside the store lock.</returns>
    public BoxedEventDispatchSnapshot CreateDispatchSnapshot(
        IEventEnvelope envelope,
        string operation)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        SelectedListeners selectedListeners;

        lock (_sync)
        {
            CheckEventPayloadTypeBinding(envelope.EventId, envelope.PayloadType, operation);
            BindEventPayloadType(envelope.EventId, envelope.PayloadType);
            selectedListeners = CollectSelectedListeners(envelope);
        }

        return selectedListeners.CreateBoxedSnapshot(envelope);
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
    /// Collects the listeners selected for one dispatch and consumes any one-shot listeners chosen
    /// for the current dispatch.
    /// </summary>
    /// <param name="envelope">The envelope being dispatched.</param>
    /// <returns>The selected listeners grouped by dispatch kind.</returns>
    private SelectedListeners CollectSelectedListeners(IEventEnvelope envelope)
    {
        var listeners = new List<EventListenerRegistration>();
        var onceListeners = new List<EventListenerRegistration>();
        if (_listeners.TryGetValue(envelope.EventId, out var registeredListeners))
        {
            listeners.AddRange(registeredListeners);
        }

        if (_onceListeners.TryGetValue(envelope.EventId, out var registeredOnceListeners))
        {
            onceListeners.AddRange(registeredOnceListeners);
            _onceListeners.Remove(envelope.EventId);
        }

        var matchedListeners = new List<MatchedListenerEntry>();
        var matchedOnceListeners = new List<MatchedListenerEntry>();
        var emptyMatchRegistrationIds = new List<string>();

        foreach (var registration in _matchListeners.Values)
        {
            if (!registration.Matcher(envelope)) { continue; }

            foreach (var listener in registration.Listeners)
            {
                matchedListeners.Add(new MatchedListenerEntry(registration.Id, listener));
            }

            foreach (var listener in registration.OnceListeners)
            {
                matchedOnceListeners.Add(new MatchedListenerEntry(registration.Id, listener));
            }

            registration.OnceListeners.Clear();
            if (registration.IsEmpty)
            {
                emptyMatchRegistrationIds.Add(registration.Id);
            }
        }

        foreach (var matchExpressionId in emptyMatchRegistrationIds)
        {
            _matchListeners.Remove(matchExpressionId);
        }

        return new SelectedListeners(
            listeners,
            onceListeners,
            matchedListeners,
            matchedOnceListeners);
    }

    /// <summary>
    /// Groups the listener registrations selected for one dispatch before they are projected into a
    /// typed or boxed snapshot.
    /// </summary>
    private sealed record SelectedListeners(
        IReadOnlyList<EventListenerRegistration> Listeners,
        IReadOnlyList<EventListenerRegistration> OnceListeners,
        IReadOnlyList<MatchedListenerEntry> MatchedListeners,
        IReadOnlyList<MatchedListenerEntry> MatchedOnceListeners)
    {
        /// <summary>
        /// Projects the selected registrations into the typed dispatch snapshot used by local emits.
        /// </summary>
        /// <typeparam name="TPayload">The payload type carried by the emitted envelope.</typeparam>
        /// <param name="envelope">The typed envelope shared by every selected listener.</param>
        /// <returns>The typed dispatch snapshot for the current emission.</returns>
        public EventDispatchSnapshot<TPayload> CreateTypedSnapshot<TPayload>(EventEnvelope<TPayload> envelope)
        {
            var listeners = new List<Action<EventEnvelope<TPayload>>>(Listeners.Count);
            foreach (var registration in Listeners)
            {
                listeners.Add(registration.GetHandler<TPayload>());
            }

            var onceListeners = new List<Action<EventEnvelope<TPayload>>>(OnceListeners.Count);
            foreach (var registration in OnceListeners)
            {
                onceListeners.Add(registration.GetHandler<TPayload>());
            }

            var matchedListeners = new List<MatchListenerDispatch<TPayload>>(MatchedListeners.Count);
            foreach (var entry in MatchedListeners)
            {
                matchedListeners.Add(new MatchListenerDispatch<TPayload>(
                    entry.MatchExpressionId,
                    entry.Registration.GetHandler<TPayload>()));
            }

            var matchedOnceListeners = new List<MatchListenerDispatch<TPayload>>(MatchedOnceListeners.Count);
            foreach (var entry in MatchedOnceListeners)
            {
                matchedOnceListeners.Add(new MatchListenerDispatch<TPayload>(entry.MatchExpressionId, entry.Registration.GetHandler<TPayload>()));
            }

            return new EventDispatchSnapshot<TPayload>(
                envelope,
                listeners,
                onceListeners,
                matchedListeners,
                matchedOnceListeners);
        }

        /// <summary>
        /// Projects the selected registrations into the erased dispatch snapshot used by transport
        /// receives.
        /// </summary>
        /// <param name="envelope">The erased envelope shared by every selected listener.</param>
        /// <returns>The erased dispatch snapshot for the current receive operation.</returns>
        public BoxedEventDispatchSnapshot CreateBoxedSnapshot(IEventEnvelope envelope)
        {
            var listeners = new List<BoxedEventListenerDispatch>(Listeners.Count);
            foreach (var registration in Listeners)
            {
                listeners.Add(new BoxedEventListenerDispatch(envelope.EventId, registration.Dispatch));
            }

            var onceListeners = new List<BoxedEventListenerDispatch>(OnceListeners.Count);
            foreach (var registration in OnceListeners)
            {
                onceListeners.Add(new BoxedEventListenerDispatch(envelope.EventId, registration.Dispatch));
            }

            var matchedListeners = new List<BoxedEventListenerDispatch>(MatchedListeners.Count);
            foreach (var entry in MatchedListeners)
            {
                matchedListeners.Add(new BoxedEventListenerDispatch(entry.MatchExpressionId, entry.Registration.Dispatch));
            }

            var matchedOnceListeners = new List<BoxedEventListenerDispatch>(MatchedOnceListeners.Count);
            foreach (var entry in MatchedOnceListeners)
            {
                matchedOnceListeners.Add(new BoxedEventListenerDispatch(entry.MatchExpressionId, entry.Registration.Dispatch));
            }

            return new BoxedEventDispatchSnapshot(
                envelope,
                listeners,
                onceListeners,
                matchedListeners,
                matchedOnceListeners);
        }
    }

    /// <summary>
    /// Stores one matched listener registration together with the match-expression id that selected it.
    /// </summary>
    private readonly record struct MatchedListenerEntry(
        string MatchExpressionId,
        EventListenerRegistration Registration);

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
            void RemoveFrom(Dictionary<string, HashSet<EventListenerRegistration>> registry)
            {
                if (!registry.TryGetValue(eventId, out var listeners)) return;

                listeners.RemoveWhere(listener => Equals(listener.Handler, handler));
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
                registration.Listeners.RemoveWhere(listener => Equals(listener.Handler, handler));
            }

            if (removeOnce)
            {
                registration.OnceListeners.RemoveWhere(listener => Equals(listener.Handler, handler));
            }

            if (registration.IsEmpty)
            {
                _matchListeners.Remove(matchExpressionId);
            }
        }
    }

    /// <summary>
    /// Records the payload type associated with an event id on first use.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
    /// <param name="eventDefinition">The event definition whose id should be bound.</param>
    private void BindEventPayloadType<TPayload>(EventDefinition<TPayload> eventDefinition)
    {
        BindPayloadType(_eventPayloadTypes, eventDefinition.Id, typeof(TPayload));
    }

    /// <summary>
    /// Records the payload type associated with an event id on first transport receive.
    /// </summary>
    /// <param name="eventId">The event id whose payload type should be bound.</param>
    /// <param name="payloadType">The erased payload type associated with the identifier.</param>
    private void BindEventPayloadType(string eventId, Type payloadType)
    {
        BindPayloadType(_eventPayloadTypes, eventId, payloadType);
    }

    /// <summary>
    /// Records the payload type associated with a match-expression id on first use.
    /// </summary>
    /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
    /// <param name="matchExpression">The match expression whose id should be bound.</param>
    private void BindMatchExpressionPayloadType<TPayload>(MatchExpression<TPayload> matchExpression)
    {
        BindPayloadType(_matchExpressionPayloadTypes, matchExpression.Id, typeof(TPayload));
    }

    /// <summary>
    /// Stores the first payload-type binding for an identifier and ignores later matching writes.
    /// </summary>
    /// <param name="registry">The identifier-to-payload-type map to update.</param>
    /// <param name="id">The event or match-expression identifier being bound.</param>
    /// <param name="payloadType">The payload type associated with the identifier.</param>
    private static void BindPayloadType(
        Dictionary<string, Type> registry,
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
        CheckPayloadTypeBinding(
            _eventPayloadTypes,
            eventDefinition,
            eventDefinition.Id,
            typeof(TPayload),
            operation);
    }

    /// <summary>
    /// Verifies that an event id is not already bound to another payload type on transport receive.
    /// </summary>
    /// <param name="eventId">The event id whose binding should be checked.</param>
    /// <param name="payloadType">The erased payload type requested by the current operation.</param>
    /// <param name="operation">The caller operation name used in the exception message.</param>
    private void CheckEventPayloadTypeBinding(
        string eventId,
        Type payloadType,
        [CallerMemberName] string operation = "")
    {
        CheckPayloadTypeBinding(
            _eventPayloadTypes,
            nameof(EventDefinition<>),
            eventId,
            payloadType,
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
        CheckPayloadTypeBinding(
            _matchExpressionPayloadTypes,
            matchExpression,
            matchExpression.Id,
            typeof(TPayload),
            operation);
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
    private static void CheckPayloadTypeBinding<TBinding>(
        Dictionary<string, Type> registry,
        TBinding binding,
        string id,
        Type currentType,
        string operation)
    {
        var bindingTarget = DescribeBindingTarget(binding);

        CheckPayloadTypeBinding(registry, bindingTarget, id, currentType, operation);
    }

    /// <summary>
    /// Throws when an identifier has already been bound to a conflicting erased payload type.
    /// </summary>
    /// <param name="registry">The identifier-to-payload-type map to inspect.</param>
    /// <param name="bindingTarget">The formatted binding target used for diagnostics.</param>
    /// <param name="id">The identifier whose payload type is being checked.</param>
    /// <param name="currentType">The payload type requested by the current operation.</param>
    /// <param name="operation">The caller operation name used in the exception message.</param>
    private static void CheckPayloadTypeBinding(
        Dictionary<string, Type> registry,
        string bindingTarget,
        string id,
        Type currentType,
        string operation)
    {
        if (!registry.TryGetValue(id, out var boundType) || boundType == currentType) return;

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

        if (genericDefinition == typeof(EventDefinition<>)) return nameof(EventDefinition<>);

        if (genericDefinition == typeof(MatchExpression<>)) return nameof(MatchExpression<>);

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
    /// Stores one listener delegate together with its AOT-safe erased dispatch callback.
    /// </summary>
    private sealed class EventListenerRegistration
    {
        private readonly Action<IEventEnvelope> _dispatch;

        private EventListenerRegistration(
            Delegate handler,
            Type payloadType,
            Action<IEventEnvelope> dispatch)
        {
            Handler = handler;
            PayloadType = payloadType;
            _dispatch = dispatch;
        }

        /// <summary>
        /// Gets the comparer that deduplicates registrations by handler delegate.
        /// </summary>
        public static IEqualityComparer<EventListenerRegistration> HandlerComparer { get; } =
            new HandlerEqualityComparer();

        /// <summary>
        /// Gets the original typed listener delegate.
        /// </summary>
        public Delegate Handler { get; }

        /// <summary>
        /// Gets the payload type accepted by the listener.
        /// </summary>
        public Type PayloadType { get; }

        /// <summary>
        /// Creates a listener registration from a typed listener.
        /// </summary>
        /// <typeparam name="TPayload">The payload type accepted by the listener.</typeparam>
        /// <param name="handler">The typed listener delegate.</param>
        /// <returns>The erased listener registration.</returns>
        public static EventListenerRegistration Create<TPayload>(Action<EventEnvelope<TPayload>> handler)
        {
            return new EventListenerRegistration(
                handler,
                typeof(TPayload),
                envelope =>
                {
                    if (envelope is not EventEnvelope<TPayload> typedEnvelope)
                    {
                        throw new EventEnvelopeTypeMismatchException(envelope, typeof(TPayload));
                    }

                    handler(typedEnvelope);
                });
        }

        /// <summary>
        /// Dispatches an erased envelope to the original typed listener.
        /// </summary>
        /// <param name="envelope">The envelope to dispatch.</param>
        public void Dispatch(IEventEnvelope envelope)
        {
            _dispatch(envelope);
        }

        /// <summary>
        /// Gets the original listener delegate in its typed form.
        /// </summary>
        /// <typeparam name="TPayload">The expected listener payload type.</typeparam>
        /// <returns>The typed listener delegate.</returns>
        public Action<EventEnvelope<TPayload>> GetHandler<TPayload>()
        {
            return Handler is Action<EventEnvelope<TPayload>> typedHandler
                ? typedHandler
                : throw new InvalidOperationException(
                $"Cannot dispatch listener with payload type '{FormatTypeName(PayloadType)}' as '{FormatTypeName(typeof(TPayload))}'.");
        }

        private sealed class HandlerEqualityComparer : IEqualityComparer<EventListenerRegistration>
        {
            public bool Equals(EventListenerRegistration? x, EventListenerRegistration? y)
            {
                return Equals(x?.Handler, y?.Handler);
            }

            public int GetHashCode(EventListenerRegistration obj)
            {
                return obj.Handler.GetHashCode();
            }
        }
    }

    /// <summary>
    /// Stores one match expression's erased matcher and subscribed delegates.
    /// </summary>
    private sealed class MatchListenerRegistration
    {
        private MatchListenerRegistration(
            string id,
            Type payloadType,
            Func<IEventEnvelope, bool> matcher)
        {
            Id = id;
            PayloadType = payloadType;
            Matcher = matcher;
        }

        /// <summary>
        /// Gets the match-expression id associated with this registration.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Gets the payload type accepted by this match expression.
        /// </summary>
        public Type PayloadType { get; }

        /// <summary>
        /// Gets the erased matcher used to test emitted envelopes.
        /// </summary>
        public Func<IEventEnvelope, bool> Matcher { get; }

        /// <summary>
        /// Gets persistent match-expression listeners.
        /// </summary>
        public HashSet<EventListenerRegistration> Listeners { get; } = new(EventListenerRegistration.HandlerComparer);

        /// <summary>
        /// Gets one-shot match-expression listeners.
        /// </summary>
        public HashSet<EventListenerRegistration> OnceListeners { get; } = new(EventListenerRegistration.HandlerComparer);

        /// <summary>
        /// Gets whether this registration has no remaining listeners.
        /// </summary>
        public bool IsEmpty => Listeners.Count == 0 && OnceListeners.Count == 0;

        /// <summary>
        /// Creates an erased match-listener registration from a typed match expression.
        /// </summary>
        /// <typeparam name="TPayload">The payload type expected by the match expression.</typeparam>
        /// <param name="matchExpression">The match expression used to select emitted envelopes.</param>
        /// <returns>A registration that can evaluate emitted envelopes without exposing generic state.</returns>
        public static MatchListenerRegistration Create<TPayload>(MatchExpression<TPayload> matchExpression)
        {
            return new MatchListenerRegistration(
                matchExpression.Id,
                typeof(TPayload),
                envelope =>
                {
                    if (envelope.PayloadType != typeof(TPayload)) return false;

                    if (envelope is not EventEnvelope<TPayload> typedEnvelope)
                    {
                        throw new EventEnvelopeTypeMismatchException(envelope, typeof(TPayload));
                    }

                    return matchExpression.Matcher(typedEnvelope);
                });
        }
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

/// <summary>
/// Contains erased listeners selected for one transport-originated envelope dispatch.
/// </summary>
/// <param name="Envelope">The original envelope supplied by the transport.</param>
/// <param name="Listeners">Persistent direct event listeners selected by event id.</param>
/// <param name="OnceListeners">One-shot direct event listeners selected by event id.</param>
/// <param name="MatchedListeners">Persistent match-expression listeners selected by matcher.</param>
/// <param name="MatchedOnceListeners">One-shot match-expression listeners selected by matcher.</param>
internal sealed record BoxedEventDispatchSnapshot(
    IEventEnvelope Envelope,
    IReadOnlyList<BoxedEventListenerDispatch> Listeners,
    IReadOnlyList<BoxedEventListenerDispatch> OnceListeners,
    IReadOnlyList<BoxedEventListenerDispatch> MatchedListeners,
    IReadOnlyList<BoxedEventListenerDispatch> MatchedOnceListeners);

/// <summary>
/// Describes an erased listener selected for dispatch.
/// </summary>
/// <param name="EventId">The listener key that should be reported to adapters.</param>
/// <param name="Handler">The erased listener callback.</param>
internal readonly record struct BoxedEventListenerDispatch(
    string EventId,
    Action<IEventEnvelope> Handler);

/// <summary>
/// Thrown when an erased envelope advertises one payload type but its runtime envelope type cannot
/// be dispatched as the expected <see cref="EventEnvelope{TBody}"/> shape.
/// </summary>
/// <remarks>
/// Creates an exception describing one envelope runtime-type mismatch.
/// </remarks>
/// <param name="envelope">The envelope whose runtime type was rejected.</param>
/// <param name="expectedPayloadType">The payload type selected by the listener.</param>
internal sealed class EventEnvelopeTypeMismatchException(IEventEnvelope envelope, Type expectedPayloadType) : InvalidOperationException(
    $"Cannot dispatch envelope for event '{envelope.EventId}' " +
    $"with payload type '{envelope.PayloadType}' because runtime " +
    $"envelope type '{envelope.GetType()}' is not compatible with " +
    $"{nameof(EventEnvelope<>)} carrying payload type '{expectedPayloadType}'.")
{
    /// <summary>
    /// Gets the event identifier carried by the rejected envelope.
    /// </summary>
    public string EventId { get; } = envelope.EventId;

    /// <summary>
    /// Gets the runtime CLR type of the rejected envelope instance.
    /// </summary>
    public Type EnvelopeRuntimeType { get; } = envelope.GetType();

    /// <summary>
    /// Gets the payload type advertised by the rejected envelope.
    /// </summary>
    public Type AdvertisedPayloadType { get; } = envelope.PayloadType;

    /// <summary>
    /// Gets the payload type expected by the listener or matcher.
    /// </summary>
    public Type ExpectedPayloadType { get; } = expectedPayloadType;
}
