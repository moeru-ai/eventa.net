using Eventa;
using EventaExample.Contracts;

namespace EventaExample.Consumers;

public sealed class InventoryProjection(IEventContext context)
{
    private readonly List<string> _changes = [];

    public IReadOnlyList<string> Changes => _changes;

    public IDisposable Start()
    {
        return context.Subscribe(
            InventoryEvents.StockAdjusted,
            envelope =>
            {
                var change = envelope.Body;
                var sign = change.Delta >= 0 ? "+" : string.Empty;

                _changes.Add($"{change.Sku}:{sign}{change.Delta} -> {change.QuantityOnHand}");
            });
    }
}
