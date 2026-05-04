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
            if (!_inflight.TryGetValue(invokeId, out var cancellationSource)) return;

            cancellationSource.Cancel();
        }
    }

    /// <summary>
    /// Stops tracking an invoke once handler execution has finished.
    /// </summary>
    /// <param name="invokeId">The completed invoke id.</param>
    /// <param name="cancellationSource">The cancellation source owned by the completed handler.</param>
    public void StopTracking(string invokeId, CancellationTokenSource cancellationSource)
    {
        lock (_sync)
        {
            if (!_inflight.TryGetValue(invokeId, out var trackedSource)) return;
            if (!ReferenceEquals(trackedSource, cancellationSource)) return;

            _inflight.Remove(invokeId);
        }
    }

    /// <summary>
    /// Cancels and disposes every tracked invoke when the handler registration is torn down.
    /// </summary>
    public void CancelAllAndDispose()
    {
        List<CancellationTokenSource> cancellationSources;

        lock (_sync)
        {
            cancellationSources = [.. _inflight.Values];
            _inflight.Clear();
        }

        foreach (var cancellationSource in cancellationSources)
        {
            cancellationSource.Cancel();
            cancellationSource.Dispose();
        }
    }
}

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
            RemoveIfCurrent(created);
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
        RemoveIfCurrent(state);
    }

    /// <summary>
    /// Removes the state only when it is still the current state for its invoke id.
    /// </summary>
    /// <param name="state">The state object that should own the map entry being removed.</param>
    private void RemoveIfCurrent(RequestStreamInvocationState<TRequest> state)
    {
        lock (_sync)
        {
            if (!_inflight.TryGetValue(state.InvokeId, out var trackedState)) return;
            if (!ReferenceEquals(trackedState, state)) return;

            _inflight.Remove(state.InvokeId);
        }
    }

    /// <summary>
    /// Aborts and disposes every inflight request-stream state when the handler registration is removed.
    /// </summary>
    public void AbortAllAndDispose()
    {
        List<RequestStreamInvocationState<TRequest>> states;

        lock (_sync)
        {
            states = [.. _inflight.Values];
            _inflight.Clear();
        }

        foreach (var state in states)
        {
            state.Abort();
            state.Dispose();
        }
    }
}
