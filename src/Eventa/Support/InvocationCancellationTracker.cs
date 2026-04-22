namespace Eventa;

/// <summary>
/// Tracks per-invoke cancellation sources for unary handlers so protocol abort messages can cancel
/// the currently executing work.
/// </summary>
internal sealed class InvocationCancellationTracker
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, CancellationTokenSource> _inflight =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Starts tracking a new invoke and returns the <see cref="CancellationTokenSource" /> that
    /// should be passed into the handler.
    /// </summary>
    /// <param name="invokeId">The protocol invoke id that owns the cancellation source.</param>
    public CancellationTokenSource BeginTracking(string invokeId)
    {
        var cancellationSource = new CancellationTokenSource();

        lock (_sync)
        {
            _inflight[invokeId] = cancellationSource;
        }

        return cancellationSource;
    }

    /// <summary>
    /// Cancels the tracked invoke when it is still inflight.
    /// </summary>
    /// <param name="invokeId">The invoke id that received a protocol abort message.</param>
    public void TryCancel(string invokeId)
    {
        lock (_sync)
        {
            if (_inflight.TryGetValue(invokeId, out var cancellationSource))
            {
                cancellationSource.Cancel();
            }
        }
    }

    /// <summary>
    /// Stops tracking an invoke once handler execution has finished.
    /// </summary>
    /// <param name="invokeId">The completed invoke id.</param>
    public void StopTracking(string invokeId)
    {
        lock (_sync)
        {
            _inflight.Remove(invokeId);
        }
    }

    /// <summary>
    /// Cancels and disposes every tracked invoke when the handler registration is torn down.
    /// </summary>
    public void CancelAllAndDispose()
    {
        lock (_sync)
        {
            foreach (var cancellationSource in _inflight.Values)
            {
                cancellationSource.Cancel();
                cancellationSource.Dispose();
            }

            _inflight.Clear();
        }
    }
}
