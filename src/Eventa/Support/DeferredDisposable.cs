namespace Eventa;

/// <summary>
/// Owns an <see cref="IDisposable" /> whose cleanup path may run before the
/// eventual resource has been attached.
/// </summary>
/// <remarks>
/// This lets callers publish a placeholder disposable into a cleanup list
/// first, then attach the real subscription once it exists, while keeping
/// disposal idempotent when cleanup wins the race.
/// </remarks>
internal sealed class DeferredDisposable : IDisposable
{
    private IDisposable? _disposable;
    private int _isAttached;
    private int _isDisposed;

    /// <summary>
    /// Attaches the concrete disposable that should be owned and later
    /// disposed by this instance.
    /// </summary>
    /// <param name="disposable">The disposable to own.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the helper is attached more than once.
    /// </exception>
    public void Attach(IDisposable disposable)
    {
        ArgumentNullException.ThrowIfNull(disposable);

        if (Interlocked.Exchange(ref _isAttached, 1) != 0)
        {
            disposable.Dispose();
            throw new InvalidOperationException("Disposable already attached.");
        }

        Interlocked.Exchange(ref _disposable, disposable);

        if (Volatile.Read(ref _isDisposed) == 0) return;

        Interlocked.Exchange(ref _disposable, null)?.Dispose();
    }

    /// <summary>
    /// Disposes the owned disposable once it has been attached, or records
    /// disposal so a later <see cref="Attach" /> call disposes immediately.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;

        Interlocked.Exchange(ref _disposable, null)?.Dispose();
    }
}
