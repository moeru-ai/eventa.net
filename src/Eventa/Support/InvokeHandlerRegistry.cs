namespace Eventa;

/// <summary>
/// Owns the event subscriptions and inflight cleanup associated with one handler registration.
/// </summary>
/// <param name="subscriptions">The protocol subscriptions that should be disposed when the handler is removed.</param>
/// <param name="cleanup">Additional cleanup that tears down inflight state after subscriptions are removed.</param>
internal sealed class HandlerRegistration(
    IReadOnlyCollection<IDisposable> subscriptions,
    Action cleanup) : IDisposable
{
    private int _disposed;

    /// <summary>
    /// Disposes the owned subscriptions once and then runs the additional inflight cleanup.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }

        cleanup();
    }
}


/// <summary>
/// Deduplicates handler registrations per event id and delegate instance for one context.
/// </summary>
/// <remarks>
/// This avoids wiring the same protocol subscriptions multiple times when a caller registers the
/// same handler repeatedly for the same send event.
/// </remarks>
internal sealed class InvokeHandlerRegistry
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, Dictionary<Delegate, HandlerRegistration>> _registrations =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Gets the existing registration for the handler or creates it once for the specified event.
    /// </summary>
    /// <param name="eventId">The send-event identifier that keys the registration bucket.</param>
    /// <param name="handler">The handler delegate whose registration should be tracked.</param>
    /// <param name="createRegistration">
    /// The factory that wires subscriptions and cleanup when no registration exists yet.
    /// </param>
    /// <returns>
    /// An <see cref="IDisposable"/> that removes the tracked handler registration when disposed.
    /// </returns>
    public IDisposable Register(
        string eventId,
        Delegate handler,
        Func<HandlerRegistration> createRegistration)
    {
        lock (_sync)
        {
            if (!_registrations.TryGetValue(eventId, out var handlers))
            {
                handlers = [];
                _registrations[eventId] = handlers;
            }

            if (!handlers.TryGetValue(handler, out HandlerRegistration? registration))
            {
                registration = createRegistration();
                handlers[handler] = registration;
            }
        }

        return new ActionDisposable(() => Remove(eventId, handler));
    }

    /// <summary>
    /// Removes one tracked handler registration and disposes its owned subscriptions after the
    /// registry lock has been released.
    /// </summary>
    /// <param name="eventId">The send-event identifier that keys the registration bucket.</param>
    /// <param name="handler">The handler delegate whose registration should be removed.</param>
    private void Remove(string eventId, Delegate handler)
    {
        HandlerRegistration? registration;

        lock (_sync)
        {
            if (!_registrations.TryGetValue(eventId, out var handlers)
                || !handlers.TryGetValue(handler, out registration)) return;

            handlers.Remove(handler);
            if (handlers.Count == 0)
            {
                _registrations.Remove(eventId);
            }
        }

        registration.Dispose();
    }
}
