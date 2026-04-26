namespace Eventa;

/// <summary>
/// Stores per-context invoke extension state under <see cref="InvokeExtensions.InternalInvokeConfigKey"/>.
/// </summary>
/// <remarks>
/// Abort registrations are collected here so <see cref="EventInvoke"/> can wire fatal-event
/// or fatal-match handling for each pending invoke without knowing the original payload type.
/// </remarks>
internal sealed class InvokeInternalConfig
{
    /// <summary>
    /// Gets the fatal event or match-expression registrations that should abort pending invokes.
    /// </summary>
    public List<AbortEventRegistration> AbortOnEvents { get; } = [];
}

/// <summary>
/// Captures how to subscribe a fatal source after its original generic payload type has been
/// erased by storage in <see cref="IEventContext.Extensions"/>.
/// </summary>
/// <remarks>
/// The registration keeps a typed subscribe callback instead of a raw event definition so the
/// invoke pipeline can stay AOT-safe and avoid rebuilding payload-specific mapping logic later.
/// </remarks>
/// <param name="id">The fatal event or match-expression identifier represented by this registration.</param>
/// <param name="kind">The source kind represented by this registration.</param>
/// <param name="subscribe">
/// The callback that subscribes the fatal source on a target context and maps it to an abort.
/// </param>
internal sealed class AbortEventRegistration(
    string id,
    AbortEventRegistrationKind kind,
    Func<IEventContext, Action<Exception?>, IDisposable> subscribe)
{
    /// <summary>
    /// Gets the source kind represented by this registration.
    /// </summary>
    public AbortEventRegistrationKind Kind { get; } = kind;

    /// <summary>
    /// Gets the fatal event or match-expression identifier represented by this registration.
    /// </summary>
    public string Id { get; } = id;

    /// <summary>
    /// Subscribes the fatal source on the supplied context and forwards observed failures to
    /// <paramref name="onAbort"/>.
    /// </summary>
    /// <param name="context">The context whose fatal event stream should be observed.</param>
    /// <param name="onAbort">The callback that receives the mapped abort exception.</param>
    /// <returns>An <see cref="IDisposable"/> that removes the fatal-event subscription.</returns>
    public IDisposable Subscribe(IEventContext context, Action<Exception?> onAbort)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(onAbort);

        return subscribe(context, onAbort);
    }
}

internal enum AbortEventRegistrationKind
{
    Event,
    MatchExpression,
}
