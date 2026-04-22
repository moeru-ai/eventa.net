namespace Eventa;

/// <summary>
/// Arms client-side cancellation for a pending invoke or stream operation and publishes the
/// disposable registration into the same cleanup list as the operation subscriptions.
/// </summary>
/// <remarks>
/// The helper centralizes the race around <see cref="CancellationToken.Register(Action)" /> so
/// callers can share one consistent pre-check, registration, and post-check sequence.
/// </remarks>
internal static class ClientCancellation
{
    /// <summary>
    /// Registers <paramref name="onCancellation" /> when the token can still be canceled and adds
    /// the owned registration to <paramref name="cleanup" />.
    /// </summary>
    /// <param name="cancellationToken">The client token that should abort the pending operation.</param>
    /// <param name="cleanup">The cleanup list that owns the eventual cancellation registration.</param>
    /// <param name="onCancellation">The callback that transitions the operation into its abort path.</param>
    /// <returns>
    /// <see langword="true" /> when the operation remains armed after registration;
    /// <see langword="false" /> when cancellation already won and <paramref name="onCancellation" />
    /// has been invoked.
    /// </returns>
    public static bool TryArm(
        CancellationToken cancellationToken,
        ICollection<IDisposable> cleanup,
        Action onCancellation)
    {
        ArgumentNullException.ThrowIfNull(cleanup);
        ArgumentNullException.ThrowIfNull(onCancellation);

        if (!cancellationToken.CanBeCanceled) return true;

        if (cancellationToken.IsCancellationRequested)
        {
            onCancellation();
            return false;
        }

        var cancellationRegistration = new DeferredCancellationRegistration();
        cleanup.Add(cancellationRegistration);
        cancellationRegistration.Attach(cancellationToken.Register(onCancellation));

        // Cancellation can still win the race between the pre-check and Register.
        if (cancellationToken.IsCancellationRequested)
        {
            onCancellation();
            return false;
        }

        return true;
    }
}
