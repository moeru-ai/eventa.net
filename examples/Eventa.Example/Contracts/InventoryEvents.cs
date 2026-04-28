using Eventa;

namespace EventaExample.Contracts;

public static class InventoryEvents
{
    public static readonly EventDefinition<StockAdjusted> StockAdjusted =
        new("example:inventory:stock-adjusted");
}

public sealed record StockAdjusted(string Sku, int Delta, int QuantityOnHand);
