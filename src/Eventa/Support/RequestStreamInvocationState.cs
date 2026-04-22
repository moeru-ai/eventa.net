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
    /// Aborts the request stream by faulting the queued input and canceling the handler token.
    /// </summary>
    public void Abort()
    {
        Requests.Fault(new OperationCanceledException(CancellationSource.Token));
        CancellationSource.Cancel();
    }

    /// <summary>
    /// Disposes the owned cancellation source after the invoke has fully completed.
    /// </summary>
    public void Dispose()
    {
        CancellationSource.Dispose();
    }
}
