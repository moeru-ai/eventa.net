namespace Eventa;

/// <summary>
/// Stores per-context invoke extension state under <see cref="InvokeExtensions.InternalInvokeConfigKey"/>.
/// </summary>
/// <remarks>
/// Abort registrations are collected here so <see cref="EventInvoke"/> can wire fatal-event
/// handling for each pending invoke without knowing the original fatal-event payload type.
/// </remarks>
internal sealed class InvokeInternalConfig
{
    public List<AbortEventRegistration> AbortOnEvents { get; } = [];
}

/// <summary>
/// Captures how to subscribe a fatal event after its original generic payload type has been erased
/// by storage in <see cref="IEventContext.Extensions"/>.
/// </summary>
/// <remarks>
/// The registration keeps a typed subscribe callback instead of a raw event definition so the
/// invoke pipeline can stay AOT-safe and avoid rebuilding payload-specific mapping logic later.
/// </remarks>
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
