namespace Eventa;

// Bounded, per-context history only: no task table, processing outcome, or rollback state.
internal sealed class RecentDeliveryHistory
{
    internal static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(5);
    internal const int DefaultCapacity = 10_000;

    private readonly Lock _sync = new();
    private readonly HashSet<string> _seen = new(StringComparer.Ordinal);
    private readonly Queue<(string Id, long Timestamp)> _oldestFirst = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _ttl;
    private readonly int _capacity;

    /// <summary>Creates a history using the system clock and the default retention and capacity limits.</summary>
    public RecentDeliveryHistory() : this(TimeProvider.System, DefaultTtl, DefaultCapacity) { }

    internal RecentDeliveryHistory(TimeProvider timeProvider, TimeSpan ttl, int capacity)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _timeProvider = timeProvider;
        _ttl = ttl;
        _capacity = capacity;
    }

    /// <summary>Atomically records an ID if no live entry already exists.</summary>
    /// <param name="deliveryId">The logical delivery identity to check and register.</param>
    /// <returns>
    /// <see langword="true"/> if newly admitted, or <see langword="false"/> for a retained duplicate.
    /// </returns>
    public bool TryRegister(string deliveryId)
    {
        lock (_sync)
        {
            // Monotonic timestamps and a shared TTL let expired entries be removed from the head.
            long now = _timeProvider.GetTimestamp();
            while (_oldestFirst.TryPeek(out (string Id, long Timestamp) oldest)
                && _timeProvider.GetElapsedTime(oldest.Timestamp, now) >= _ttl)
            {
                _seen.Remove(_oldestFirst.Dequeue().Id);
            }

            // Duplicates neither renew their own retention nor evict another ID.
            if (_seen.Contains(deliveryId)) return false;

            if (_seen.Count == _capacity)
            {
                _seen.Remove(_oldestFirst.Dequeue().Id);
            }

            _seen.Add(deliveryId);
            _oldestFirst.Enqueue((deliveryId, now));
            return true;
        }
    }
}
