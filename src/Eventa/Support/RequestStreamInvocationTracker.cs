namespace Eventa;

/// <summary>
/// Tracks per-invoke state for handlers that consume a request stream and lazily starts handler
/// execution when the first protocol message for an invoke arrives.
/// </summary>
/// <typeparam name="TRequest">The request payload type queued into each invoke state.</typeparam>
internal sealed class RequestStreamInvocationTracker<TRequest>
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, RequestStreamInvocationState<TRequest>> _inflight =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Gets the existing state for <paramref name="invokeId" /> or creates it, publishes it
    /// under the tracker lock, and starts handler execution exactly once after the lock is released.
    /// If <paramref name="startExecution" /> throws synchronously for a newly-created state, the
    /// tracker removes and disposes that state before rethrown.
    /// </summary>
    /// <param name="invokeId">The invoke id whose request-stream state should be resolved.</param>
    /// <param name="startExecution">The callback that starts handler execution for a newly created state.</param>
    public RequestStreamInvocationState<TRequest> GetOrCreate(
        string invokeId,
        Func<RequestStreamInvocationState<TRequest>, Task> startExecution)
    {
        ArgumentNullException.ThrowIfNull(startExecution);

        RequestStreamInvocationState<TRequest>? created;

        lock (_sync)
        {
            if (_inflight.TryGetValue(invokeId, out var existing)) return existing;

            created = new RequestStreamInvocationState<TRequest>(invokeId);
            _inflight[invokeId] = created;
        }

        try
        {
            // State publication must stay under the tracker lock, but handler startup must not.
            created.Execution = startExecution(created);
            return created;
        }
        catch
        {
            lock (_sync)
            {
                if (_inflight.TryGetValue(invokeId, out var existing)
                    && ReferenceEquals(existing, created))
                {
                    _inflight.Remove(invokeId);
                }
            }

            created.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Removes a completed invoke from the inflight state map.
    /// </summary>
    /// <param name="state">The state object that has finished executing.</param>
    public void Remove(RequestStreamInvocationState<TRequest> state)
    {
        lock (_sync)
        {
            _inflight.Remove(state.InvokeId);
        }
    }

    /// <summary>
    /// Aborts and disposes every inflight request-stream state when the handler registration is removed.
    /// </summary>
    public void AbortAllAndDispose()
    {
        lock (_sync)
        {
            foreach (var state in _inflight.Values)
            {
                state.Abort();
                state.Dispose();
            }

            _inflight.Clear();
        }
    }
}
