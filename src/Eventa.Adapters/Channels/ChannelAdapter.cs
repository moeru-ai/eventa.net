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

        var message = new ChannelMessage(eventEnvelope, options);
        if (outbound.TryWrite(message)) return;

        // Keep Emit synchronous: probe terminal state vs. transient pressure without waiting.
        var waitToWrite = outbound.WaitToWriteAsync();
        if (!waitToWrite.IsCompleted)
        {
            throw new InvalidOperationException("Outbound channel could not accept the message immediately.");
        }

        if (waitToWrite.IsCompletedSuccessfully)
        {
            // ValueTask can be backed by IValueTaskSource, so consume the completed result once.
            var canWrite = waitToWrite.Result;

            // WaitToWriteAsync(true) only reports a writable window; another writer can still win
            // that race before we claim the slot, so we need one more non-blocking write attempt.
            if (canWrite && outbound.TryWrite(message)) return;

            if (canWrite)
            {
                throw new InvalidOperationException("Outbound channel could not accept the message immediately.");
            }
        }

        // A synchronously completed false/faulted wait means the writer is terminal. Use WriteAsync
        // here so completed/faulted channels surface their original exception shape unchanged.
        outbound.WriteAsync(message).AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public void OnReceived(string eventId, object? envelope, object? options = null) { }

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Exchange(ref _disposed, 1);
    }
}
