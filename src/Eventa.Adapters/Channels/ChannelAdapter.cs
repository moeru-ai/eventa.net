using System.Threading.Channels;

using Eventa;

namespace Eventa.Adapters.Channels;

/// <summary>
/// Mirrors locally sent Eventa envelopes to an outbound channel writer.
/// </summary>
internal sealed class ChannelAdapter(ChannelWriter<ChannelMessage> outbound) : IEventaAdapter
{
    private int _disposed;

    /// <inheritdoc />
    public void OnSent(string eventId, object? envelope, object? options = null)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ChannelClosedException("Channel endpoint closed.");
        }

        if (envelope is not IEventEnvelope eventEnvelope)
        {
            throw new InvalidOperationException(
                $"Event '{eventId}' was sent with an envelope that does not implement {nameof(IEventEnvelope)}.");
        }

        outbound.WriteAsync(new ChannelMessage(eventEnvelope, options))
            .AsTask()
            .GetAwaiter()
            .GetResult();
    }

    /// <inheritdoc />
    public void OnReceived(string eventId, object? envelope) { }

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Exchange(ref _disposed, 1);
    }
}
