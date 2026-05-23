using System.Diagnostics.CodeAnalysis;

namespace Eventa;

/// <summary>
/// Centralizes typed access to <see cref="IEventContext.Extensions"/> for internal features.
/// </summary>
internal static class EventContextFeatures
{
    /// <summary>
    /// Resolves a typed context feature by key, creating or replacing the stored value when needed.
    /// </summary>
    public static TFeature GetOrCreateFeature<TFeature>(
        this IEventContext context,
        string key)
        where TFeature : class, new()
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrEmpty(key);

        lock (context.Extensions)
        {
            if (context.Extensions.TryGetValue(key, out var rawFeature)
                && rawFeature is TFeature feature)
            {
                return feature;
            }

            feature = new TFeature();
            context.Extensions[key] = feature;
            return feature;
        }
    }

    /// <summary>
    /// Tries to resolve a typed context feature by key without creating it.
    /// </summary>
    public static bool TryGetFeature<TFeature>(
        this IEventContext context,
        string key,
        [NotNullWhen(true)] out TFeature? feature)
        where TFeature : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrEmpty(key);

        lock (context.Extensions)
        {
            if (context.Extensions.TryGetValue(key, out var rawFeature)
                && rawFeature is TFeature typedFeature)
            {
                feature = typedFeature;
                return true;
            }
        }

        feature = null;
        return false;
    }
}

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

    /// <summary>
    /// Gets the transport-fatal callback registry for pending invokes on this context.
    /// </summary>
    public TransportFatalInvocationRegistry TransportFatalInvocations { get; } = new();
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

/// <summary>
/// Stores active invoke-session callbacks that should fault when the owning transport terminates.
/// </summary>
internal sealed class TransportFatalInvocationRegistry
{
    private readonly Lock _sync = new();
    private readonly Dictionary<int, Action<Exception>> _callbacks = [];
    private int _nextId;

    /// <summary>
    /// Registers one active invoke-session callback.
    /// </summary>
    /// <param name="onFatal">The callback to invoke when the transport terminates.</param>
    /// <returns>A disposable that removes the callback.</returns>
    public IDisposable Register(Action<Exception> onFatal)
    {
        ArgumentNullException.ThrowIfNull(onFatal);

        int id;
        lock (_sync)
        {
            id = ++_nextId;
            _callbacks[id] = onFatal;
        }

        return new ActionDisposable(() =>
        {
            lock (_sync)
            {
                _callbacks.Remove(id);
            }
        });
    }

    /// <summary>
    /// Notifies every active invoke session of the terminal transport error.
    /// </summary>
    /// <param name="error">The terminal transport error.</param>
    public void Notify(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        List<Action<Exception>> callbacks;
        lock (_sync)
        {
            callbacks = [.. _callbacks.Values];
        }

        foreach (var callback in callbacks)
        {
            callback(error);
        }
    }
}
