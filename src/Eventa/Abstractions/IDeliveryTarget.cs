namespace Eventa;

/// <summary>Accepts existing deliveries for local dispatch.</summary>
public interface IDeliveryTarget
{
    /// <summary>
    /// Submits a delivery and invokes local listeners for its event identifier if its ID is unseen.
    /// </summary>
    /// <param name="delivery">A delivery created locally or validated by <see cref="Delivery.Restore"/>.</param>
    /// <returns>
    /// A task that succeeds when all selected listeners succeed. An unhandled event or a recently seen
    /// delivery ID completes successfully without dispatch. If a listener fails, the task faults
    /// after every selected listener has been invoked, even if some listener tasks are still pending.
    /// A canceled listener task counts as a failure. Awaiting the returned task propagates the selected
    /// listener exception, including the cancellation exception if that listener's failure is selected.
    /// </returns>
    /// <remarks>
    /// Duplicates complete independently of any earlier submission. Local dispatch preserves the hop
    /// budget, including at zero hops. Application code can check for cancellation before submission.
    /// Started listeners continue their work if another listener fails.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="delivery"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The returned task may fault with this exception when a listener returns a null task.
    /// </exception>
    Task AcceptAsync(Delivery delivery);
}
