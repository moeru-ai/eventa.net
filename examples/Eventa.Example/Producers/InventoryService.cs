using Eventa;
using EventaExample.Contracts;

namespace EventaExample.Producers;

public sealed class InventoryService(IEventContext context)
{
    private readonly Dictionary<string, int> _stock = new(StringComparer.OrdinalIgnoreCase);

    public void AdjustStock(string sku, int delta)
    {
        var quantityOnHand = _stock.GetValueOrDefault(sku) + delta;

        _stock[sku] = quantityOnHand;
        context.Emit(
            InventoryEvents.StockAdjusted,
            new StockAdjusted(sku, delta, quantityOnHand));
    }
}
