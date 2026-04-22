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
    /// Gets the existing state for <paramref name="invokeId" /> or creates it and starts handler
    /// execution exactly once.
    /// </summary>
    /// <param name="invokeId">The invoke id whose request-stream state should be resolved.</param>
    /// <param name="startExecution">The callback that starts handler execution for a newly created state.</param>
    public RequestStreamInvocationState<TRequest> GetOrCreate(
        string invokeId,
        Func<RequestStreamInvocationState<TRequest>, Task> startExecution)
    {
        ArgumentNullException.ThrowIfNull(startExecution);

        lock (_sync)
        {
            if (_inflight.TryGetValue(invokeId, out var existing)) return existing;

            var created = new RequestStreamInvocationState<TRequest>(invokeId);
            _inflight[invokeId] = created;
            created.Execution = startExecution(created);
            return created;
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
