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
