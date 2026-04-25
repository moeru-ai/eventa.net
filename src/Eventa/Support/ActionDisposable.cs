namespace Eventa;

/// <summary>
/// Wraps a callback so only the first invocation can execute it.
/// </summary>
/// <param name="action">The callback that should run at most once.</param>
internal sealed class RunOnceAction(Action action)
{
    private Action? _action = action;

    /// <summary>
    /// Invokes the wrapped callback exactly once. Later calls are no-ops.
    /// </summary>
    public void Invoke()
    {
        Interlocked.Exchange(ref _action, null)?.Invoke();
    }
}

/// <summary>
/// Wraps a cleanup callback in an <see cref="IDisposable"/> so callers can expose
/// one-time teardown logic such as unregistering an event handler.
/// </summary>
/// <param name="dispose">The callback to invoke the first time <see cref="Dispose"/> is called.</param>
internal sealed class ActionDisposable(Action dispose) : IDisposable
{
    private readonly RunOnceAction _dispose = new(dispose);

    /// <summary>
    /// Invokes the stored cleanup callback once and prevents subsequent calls from running it again.
    /// </summary>
    public void Dispose()
    {
        _dispose.Invoke();
    }
}
