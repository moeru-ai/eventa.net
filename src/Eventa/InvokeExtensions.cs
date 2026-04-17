namespace Eventa;

public static class InvokeExtensions
{
    internal const string InternalInvokeConfigKey = "__internal.invoke";

    public static void RegisterAbortEvent(
        this IEventContext context,
        EventDefinition<object> fatalEvent)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(fatalEvent);

        if (!context.Extensions.TryGetValue(InternalInvokeConfigKey, out var rawConfig)
            || rawConfig is not InvokeInternalConfig config)
        {
            config = new InvokeInternalConfig();
            context.Extensions[InternalInvokeConfigKey] = config;
        }

        if (!config.AbortOnEvents.Any(existing => existing.Id == fatalEvent.Id))
        {
            config.AbortOnEvents.Add(fatalEvent);
        }
    }
}
