namespace Eventa;

public sealed partial class EventContext
{
    private readonly RecentDeliveryHistory _recentDeliveries = new();

    // Deterministic expiry/capacity tests without adding a public clock/configuration API.
    internal EventContext(TimeProvider timeProvider, TimeSpan recentDeliveryTtl, int recentDeliveryLimit)
        : this()
    {
        _recentDeliveries = new RecentDeliveryHistory(timeProvider, recentDeliveryTtl, recentDeliveryLimit);
    }

    /// <summary>Creates a new opaque delivery and accepts it locally without consuming hops.</summary>
    /// <param name="contentType">A valid media type that describes the body representation.</param>
    /// <param name="eventa">The event and stable body to deliver to local listeners.</param>
    /// <returns>A task that follows the listener completion rules of <see cref="AcceptAsync"/>.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="contentType"/> is not a valid media type.</exception>
    public Task EmitAsync(string contentType, EventMessage eventa)
    {
        return AcceptAsync(Delivery.Create(contentType, eventa));
    }

    /// <summary>Registers a listener for deliveries with the specified event identifier.</summary>
    /// <param name="eventId">
    /// The identifier to match using ordinal, case-sensitive comparison. Empty and whitespace identifiers are valid.
    /// </param>
    /// <param name="handler">The callback that receives the delivery and returns a non-null task.</param>
    /// <returns>A subscription that removes this listener when disposed.</returns>
    /// <remarks>
    /// The callback receives the original delivery, event, and body references. Optional business
    /// decoding belongs here, after deduplication and routing. Disposing the subscription only affects
    /// future listener selection. Callbacks already selected for dispatch can still run.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public IDisposable Subscribe(string eventId, Func<Delivery, Task> handler)
    {
        return _listeners.SubscribeDelivery(eventId, handler);
    }

    /// <inheritdoc />
    public Task AcceptAsync(Delivery delivery)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        // Record admission before listener selection so even unhandled or failed deliveries stay seen.
        // An already-seen ID has no dependency on the first submission's listener tasks.
        if (!_recentDeliveries.TryRegister(delivery.deliveryId)) return Task.CompletedTask;

        Func<Delivery, Task>[] listeners = _listeners.SnapshotDeliveryListeners(delivery.eventa.id);
        if (listeners.Length == 0) return Task.CompletedTask;

        // Give every selected listener an invocation opportunity before publishing any failure.
        Task[] pending = new Task[listeners.Length];
        for (int index = 0; index < listeners.Length; index++)
        {
            try
            {
                pending[index] = listeners[index](delivery)
                    ?? Task.FromException(new InvalidOperationException("A listener returned a null Task."));
            }
            catch (Exception error)
            {
                pending[index] = Task.FromException(error);
            }
        }

        return DeliveryDispatch.CompleteAsync(pending);
    }
}
