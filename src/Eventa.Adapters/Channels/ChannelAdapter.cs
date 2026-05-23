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
        // If the probe would block, cancel it so bounded channels don't retain an abandoned
        // WaitToWriteAsync waiter after we've already decided to fail fast.
        using var waitToWriteCancellation = new CancellationTokenSource();
        var waitToWrite = outbound.WaitToWriteAsync(waitToWriteCancellation.Token);
        if (!waitToWrite.IsCompleted)
        {
            CancelAndObservePendingWait(waitToWrite, waitToWriteCancellation);
            throw new InvalidOperationException("Outbound channel could not accept the message immediately.");
        }

        // Short-circuit evaluation keeps ValueTask.Result consumed at most once, and only after
        // the wait has completed successfully.
        if (waitToWrite.IsCompletedSuccessfully && waitToWrite.Result)
        {
            // WaitToWriteAsync(true) only reports a writable window; another writer can still win
            // that race before we claim the slot, so we need one more non-blocking write attempt.
            if (outbound.TryWrite(message)) return;

            throw new InvalidOperationException("Outbound channel could not accept the message immediately.");
        }

        // A synchronously completed false/faulted wait means the writer is terminal. Use WriteAsync
        // here so completed/faulted channels surface their original exception shape unchanged.
        outbound.WriteAsync(message).AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public void OnReceived(string eventId, object? envelope, object? options = null) { }

    private static void CancelAndObservePendingWait(
        ValueTask<bool> waitToWrite,
        CancellationTokenSource waitToWriteCancellation)
    {
        // Consume the pending wait even after canceling it so pooled
        // IValueTaskSource-backed channel waiters are not abandoned.
        var waitToWriteTask = waitToWrite.AsTask();
        waitToWriteCancellation.Cancel();

        _ = waitToWriteTask.ContinueWith(
            static task =>
            {
                if (!task.IsFaulted) return;
                _ = task.Exception;
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Exchange(ref _disposed, 1);
    }
}
