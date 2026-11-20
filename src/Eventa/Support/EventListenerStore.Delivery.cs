namespace Eventa;

internal sealed partial class EventListenerStore
{
    // The opaque path shares the context's listener ownership and synchronization, but never
    // participates in the legacy typed-envelope registries or payload-type bindings.
    private readonly Dictionary<string, List<DeliveryListenerRegistration>> _deliveryListeners = new(
        StringComparer.Ordinal
    );

    /// <summary>Registers a callback for the specified delivery event identifier.</summary>
    /// <param name="eventId">The identifier to match using ordinal, case-sensitive comparison.</param>
    /// <param name="handler">The callback to invoke with each matching delivery.</param>
    /// <returns>A subscription that removes this registration when disposed.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="eventId"/> or <paramref name="handler"/> is <see langword="null"/>.
    /// </exception>
    public IDisposable SubscribeDelivery(string eventId, Func<Delivery, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(eventId);
        ArgumentNullException.ThrowIfNull(handler);
        DeliveryListenerRegistration registration = new(handler);
        lock (_sync)
        {
            if (!_deliveryListeners.TryGetValue(eventId, out List<DeliveryListenerRegistration>? listeners))
            {
                listeners = [];
                _deliveryListeners.Add(eventId, listeners);
            }

            listeners.Add(registration);
        }

        return new ActionDisposable(() =>
        {
            lock (_sync)
            {
                if (!_deliveryListeners.TryGetValue(eventId, out List<DeliveryListenerRegistration>? listeners)) return;
                listeners.Remove(registration);

                if (listeners.Count != 0) return;
                _deliveryListeners.Remove(eventId);
            }
        });
    }

    /// <summary>Captures the currently registered delivery callbacks under the store lock.</summary>
    /// <param name="eventId">The identifier whose callbacks should be selected.</param>
    /// <returns>An independent array in registration order, or an empty array if no callbacks match.</returns>
    /// <remarks>Callbacks may add or remove listeners while the snapshot is being dispatched.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="eventId"/> is <see langword="null"/>.</exception>
    public Func<Delivery, Task>[] SnapshotDeliveryListeners(string eventId)
    {
        lock (_sync)
        {
            return _deliveryListeners.TryGetValue(eventId, out List<DeliveryListenerRegistration>? listeners)
                ? [.. listeners.Select(registration => registration.Handler)]
                : [];
        }
    }

    private sealed class DeliveryListenerRegistration(Func<Delivery, Task> handler)
    {
        public Func<Delivery, Task> Handler { get; } = handler;
    }
}
