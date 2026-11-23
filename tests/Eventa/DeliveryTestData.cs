namespace Eventa.Tests;

internal sealed record DeliveryInput(
    string? contentType,
    string? deliveryId,
    int hopsRemaining,
    EventMessage? eventa) : IDeliveryData;

internal sealed class ManualDeliveryTime : TimeProvider
{
    private long _timestamp;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

    public void Advance(TimeSpan elapsed) => Interlocked.Add(ref _timestamp, elapsed.Ticks);
}
