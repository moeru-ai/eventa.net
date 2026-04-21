namespace Eventa;

internal sealed class InvokeInternalConfig
{
    public List<AbortEventRegistration> AbortOnEvents { get; } = [];
}

internal sealed class AbortEventRegistration(
    string eventId,
    Func<IEventContext, Action<Exception?>, IDisposable> subscribe)
{
    public string EventId { get; } = eventId;

    public IDisposable Subscribe(IEventContext context, Action<Exception?> onAbort)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(onAbort);

        return subscribe(context, onAbort);
    }
}
