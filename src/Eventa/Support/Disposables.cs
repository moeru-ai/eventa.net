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

/// <summary>
/// Owns an <see cref="IDisposable" /> whose cleanup path may run before the
/// eventual resource has been attached.
/// </summary>
/// <remarks>
/// This lets callers publish a placeholder disposable into a cleanup list
/// first, then attach the real subscription or cancellation registration once
/// it exists, while keeping disposal idempotent when cleanup wins the race.
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

/// <summary>
/// Arms client-side cancellation for a pending invoke or stream operation and publishes the
/// disposable registration into the same cleanup list as the operation subscriptions.
/// </summary>
/// <remarks>
/// The helper centralizes the race around <see cref="CancellationToken.Register(Action)" /> so
/// callers can share one consistent pre-check, registration, and post-check sequence. The
/// supplied <paramref name="onCancellation" /> callback is guaranteed to run at most once
/// across those three paths.
/// </remarks>
internal static class ClientCancellation
{
    /// <summary>
    /// Registers <paramref name="onCancellation" /> when the token can still be canceled and adds
    /// the owned registration to <paramref name="cleanup" />.
    /// </summary>
    /// <param name="cleanup">The cleanup list that owns the eventual cancellation registration.</param>
    /// <param name="onCancellation">The callback that transitions the operation into its abort path.</param>
    /// <param name="cancellationToken">The client token that should abort the pending operation.</param>
    /// <returns>
    /// <see langword="true" /> when the operation remains armed after registration;
    /// <see langword="false" /> when cancellation already won and <paramref name="onCancellation" />
    /// has been invoked.
    /// </returns>
    public static bool TryArm(
        ICollection<IDisposable> cleanup,
        Action onCancellation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cleanup);
        ArgumentNullException.ThrowIfNull(onCancellation);

        if (!cancellationToken.CanBeCanceled) return true;

        var guardedCancellation = new RunOnceAction(onCancellation);

        if (cancellationToken.IsCancellationRequested)
        {
            guardedCancellation.Invoke();
            return false;
        }

        var cancellationRegistration = new DeferredDisposable();
        cleanup.Add(cancellationRegistration);
        cancellationRegistration.Attach(cancellationToken.Register(guardedCancellation.Invoke));

        // Cancellation can still win the race between the pre-check and Register.
        if (cancellationToken.IsCancellationRequested)
        {
            guardedCancellation.Invoke();
            return false;
        }

        return true;
    }
}
