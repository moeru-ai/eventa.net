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
