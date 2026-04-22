namespace Eventa;

/// <summary>
/// Owns a <see cref="CancellationTokenRegistration" /> whose cleanup path may
/// run before <see cref="CancellationToken.Register(Action)" /> returns.
/// </summary>
/// <remarks>
/// Client cancellation can be observed synchronously during
/// <see cref="CancellationToken.Register(Action)" />. This helper lets callers
/// publish the eventual registration to their cleanup list first, then attach
/// the real registration once it exists, while keeping disposal idempotent.
/// </remarks>
internal sealed class DeferredCancellationRegistration : IDisposable
{
    private CancellationTokenRegistration _registration;
    private int _isAttached;
    private int _isDisposed;

    /// <summary>
    /// Attaches the concrete registration returned by
    /// <see cref="CancellationToken.Register(Action)" />.
    /// </summary>
    /// <param name="registration">The registration to own and later dispose.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the helper is attached more than once.
    /// </exception>
    public void Attach(CancellationTokenRegistration registration)
    {
        if (Interlocked.Exchange(ref _isAttached, 1) != 0)
        {
            registration.Dispose();
            throw new InvalidOperationException("Cancellation registration already attached.");
        }

        _registration = registration;

        if (Volatile.Read(ref _isDisposed) == 0) return;

        registration.Dispose();
    }

    /// <summary>
    /// Disposes the owned registration once it has been attached, or records
    /// disposal so a later <see cref="Attach" /> call disposes immediately.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;

        if (Volatile.Read(ref _isAttached) == 0) return;

        _registration.Dispose();
    }
}
