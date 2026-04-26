namespace Eventa;

/// <summary>
/// Adds fatal-event registrations that can fault pending invokes for a context.
/// </summary>
/// <remarks>
/// These registrations extend the invoke pipeline through <see cref="IEventContext.Extensions"/>
/// so callers can translate transport- or host-level fatal events into invoke failures.
/// </remarks>
public static class InvokeExtensions
{
    /// <summary>
    /// The extension-bag key that stores per-context invoke abort registrations.
    /// </summary>
    internal const string InternalInvokeConfigKey = "__internal.invoke";

    /// <summary>
    /// Registers a fatal event that faults pending invokes when it is observed.
    /// </summary>
    /// <typeparam name="TPayload">The payload type carried by the fatal event.</typeparam>
    /// <param name="context">The context whose pending invokes should observe the fatal event.</param>
    /// <param name="fatalEvent">The event that should be treated as a fatal invoke abort signal.</param>
    /// <param name="mapError">
    /// An optional payload-to-exception mapper. Use this when the fatal-event payload wraps the
    /// underlying exception instead of being an <see cref="Exception"/> itself.
    /// </param>
    /// <remarks>
    /// When <paramref name="mapError"/> is omitted, the default mapper preserves the exception only
    /// when <typeparamref name="TPayload"/> is emitted as an <see cref="Exception"/> instance. Other
    /// payload shapes fall back to Eventa's default abort exception. This default behavior is kept
    /// explicit so abort handling stays AOT-safe and does not rely on reflection-based property
    /// probing such as looking for an <c>Error</c> member by name.
    /// </remarks>
    public static void RegisterAbortEvent<TPayload>(
        this IEventContext context,
        EventDefinition<TPayload> fatalEvent,
        Func<TPayload, Exception?>? mapError = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(fatalEvent);

        mapError ??= static payload => payload is Exception exception ? exception : null;

        var config = GetOrCreateInvokeInternalConfig(context);
        var alreadyRegistered = config.AbortOnEvents.Any(existing =>
            existing.Kind == AbortEventRegistrationKind.Event
            && existing.Id == fatalEvent.Id);

        if (alreadyRegistered) return;

        var registration = new AbortEventRegistration(
            id: fatalEvent.Id,
            kind: AbortEventRegistrationKind.Event,
            (targetContext, onAbort) => targetContext.Subscribe(fatalEvent, envelope =>
            {
                onAbort(mapError(envelope.Body));
            }));

        config.AbortOnEvents.Add(registration);
    }

    /// <summary>
    /// Registers a fatal match expression that faults pending invokes when it is observed.
    /// </summary>
    /// <typeparam name="TPayload">The payload type matched by the fatal expression.</typeparam>
    /// <param name="context">The context whose pending invokes should observe the fatal expression.</param>
    /// <param name="fatalMatch">The match expression that should be treated as a fatal invoke abort signal.</param>
    /// <param name="mapError">
    /// An optional payload-to-exception mapper. Use this when the fatal payload wraps the
    /// underlying exception instead of being an <see cref="Exception"/> itself.
    /// </param>
    /// <remarks>
    /// When <paramref name="mapError"/> is omitted, the default mapper preserves the exception only
    /// when <typeparamref name="TPayload"/> is emitted as an <see cref="Exception"/> instance. Other
    /// payload shapes fall back to Eventa's default abort exception.
    /// </remarks>
    public static void RegisterAbortEvent<TPayload>(
        this IEventContext context,
        MatchExpression<TPayload> fatalMatch,
        Func<TPayload, Exception?>? mapError = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(fatalMatch);

        mapError ??= static payload => payload is Exception exception ? exception : null;

        var config = GetOrCreateInvokeInternalConfig(context);
        var alreadyRegistered = config.AbortOnEvents.Any(existing =>
            existing.Kind == AbortEventRegistrationKind.MatchExpression
            && existing.Id == fatalMatch.Id);

        if (alreadyRegistered) return;

        var registration = new AbortEventRegistration(
            id: fatalMatch.Id,
            kind: AbortEventRegistrationKind.MatchExpression,
            (targetContext, onAbort) => targetContext.Subscribe(fatalMatch, envelope =>
            {
                onAbort(mapError(envelope.Body));
            }));

        config.AbortOnEvents.Add(registration);
    }

    /// <summary>
    /// Registers an object-typed fatal event using the default error mapper.
    /// </summary>
    /// <param name="context">The context whose pending invokes should observe the fatal event.</param>
    /// <param name="fatalEvent">The event that should be treated as a fatal invoke abort signal.</param>
    /// <remarks>
    /// This convenience overload is best suited for payloads that are emitted as an
    /// <see cref="Exception"/> instance. If the payload stores the exception inside another object
    /// shape, call <see cref="RegisterAbortEvent{TPayload}(IEventContext, EventDefinition{TPayload}, Func{TPayload, Exception})"/>
    /// with an explicit mapper instead.
    /// </remarks>
    public static void RegisterAbortEvent(
        this IEventContext context,
        EventDefinition<object> fatalEvent)
    {
        RegisterAbortEvent<object>(context, fatalEvent);
    }

    /// <summary>
    /// Resolves the per-context invoke extension state, creating it on first use.
    /// </summary>
    /// <param name="context">The context whose extension bag should hold the invoke config.</param>
    /// <returns>The existing or newly created invoke extension state for the context.</returns>
    private static InvokeInternalConfig GetOrCreateInvokeInternalConfig(IEventContext context)
    {
        var hasConfig = context.Extensions.TryGetValue(InternalInvokeConfigKey, out var rawConfig);
        if (!hasConfig || rawConfig is not InvokeInternalConfig config)
        {
            config = new InvokeInternalConfig();
            context.Extensions[InternalInvokeConfigKey] = config;
        }

        return config;
    }
}
