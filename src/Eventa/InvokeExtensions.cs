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

        if (!context.Extensions.TryGetValue(InternalInvokeConfigKey, out var rawConfig)
            || rawConfig is not InvokeInternalConfig config)
        {
            config = new InvokeInternalConfig();
            context.Extensions[InternalInvokeConfigKey] = config;
        }

        if (!config.AbortOnEvents.Any(existing => existing.EventId == fatalEvent.Id))
        {
            var registration = new AbortEventRegistration(
                fatalEvent.Id,
                (targetContext, onAbort) => targetContext.On(fatalEvent, envelope =>
                {
                    onAbort(mapError(envelope.Body));
                }));

            config.AbortOnEvents.Add(registration);
        }
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
}
