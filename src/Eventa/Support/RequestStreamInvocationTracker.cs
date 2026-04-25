namespace Eventa;

/// <summary>
/// Holds the mutable per-invoke state for handlers that consume a request stream.
/// </summary>
/// <param name="invokeId">The protocol invoke id that owns this request-stream state.</param>
/// <typeparam name="TRequest">The request payload type queued for the handler.</typeparam>
internal sealed class RequestStreamInvocationState<TRequest>(string invokeId) : IDisposable
{
    /// <summary>
    /// Gets the invoke id associated with this state object.
    /// </summary>
    public string InvokeId { get; } = invokeId;

    /// <summary>
    /// Gets the queue that buffers protocol request items until the handler consumes them.
    /// </summary>
    public AsyncSignalQueue<TRequest> Requests { get; } = new();

    /// <summary>
    /// Gets the cancellation source that backs the handler's cancellation token.
    /// </summary>
    public CancellationTokenSource CancellationSource { get; } = new();

    /// <summary>
    /// Gets or sets the task that runs the handler for this invoke once it has been started.
    /// </summary>
    public Task? Execution { get; set; }

    /// <summary>
    /// Aborts the request stream by canceling the handler token and faulting the queued input.
    /// </summary>
    public void Abort()
    {
        // Handler-side cancellation filters inspect the token when the queue fault is observed,
        // so publish cancellation before surfacing the fault to request consumers.
        CancellationSource.Cancel();
        Requests.Fault(new OperationCanceledException(CancellationSource.Token));
    }

    /// <summary>
    /// Disposes the owned cancellation source after the invoke has fully completed.
    /// </summary>
    public void Dispose()
    {
        CancellationSource.Dispose();
    }
}

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
    /// tracker removes and disposes that state before rethrowing.
    /// </summary>
    /// <param name="invokeId">The invoke id whose request-stream state should be resolved.</param>
    /// <param name="startExecution">The callback that starts handler execution for a newly created state.</param>
    public RequestStreamInvocationState<TRequest> GetOrCreate(
        string invokeId,
        Func<RequestStreamInvocationState<TRequest>, Task> startExecution)
    {
        ArgumentNullException.ThrowIfNull(startExecution);

        RequestStreamInvocationState<TRequest> created;

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
