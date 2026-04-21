namespace Eventa;

public static class InvokeExtensions
{
    internal const string InternalInvokeConfigKey = "__internal.invoke";

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

    public static void RegisterAbortEvent(
        this IEventContext context,
        EventDefinition<object> fatalEvent)
    {
        RegisterAbortEvent<object>(context, fatalEvent);
    }
}
