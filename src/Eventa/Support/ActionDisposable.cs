namespace Eventa;

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
