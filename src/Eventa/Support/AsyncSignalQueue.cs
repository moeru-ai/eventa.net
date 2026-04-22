using System.Threading.Channels;

namespace Eventa;

/// <summary>
/// Bridges push-style producers to <see cref="IAsyncEnumerable{T}" /> via
/// <see cref="Channel{T}" />.
/// Normal completion and faults are represented by the channel's terminal
/// state instead of in-band sentinel values, so once completion starts, later
/// writes are rejected by the writer itself.
/// </summary>
internal sealed class AsyncSignalQueue<T>
{
    private readonly Channel<T> _channel = Channel.CreateUnbounded<T>();

    /// <summary>
    /// Attempts to enqueue a value.
    /// Returns <see langword="false" /> once the underlying channel has already
    /// completed or faulted.
    /// </summary>
    public bool TryWrite(T value)
    {
        return _channel.Writer.TryWrite(value);
    }

    /// <summary>
    /// Completes the queue normally.
    /// Buffered values remain readable, and subsequent terminal transitions are
    /// ignored by <see cref="ChannelWriter{T}.TryComplete(System.Exception?)" />.
    /// </summary>
    public void Complete()
    {
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Completes the queue with a producer error.
    /// Buffered values remain readable, after which consumers observe the
    /// original exception from channel completion.
    /// </summary>
    public void Fault(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        _channel.Writer.TryComplete(error);
    }

    /// <summary>
    /// Exposes the channel as an async sequence.
    /// When enumeration stops early, <paramref name="onDispose" /> notifies the
    /// owner; once normal completion or a producer fault is observed, disposal
    /// becomes a no-op.
    /// </summary>
    public IAsyncEnumerable<T> ReadAll(
        bool respectConsumerCancellation = true,
        Func<ValueTask>? onDispose = null)
    {
        return new AsyncSignalEnumerable<T>(_channel.Reader, respectConsumerCancellation, onDispose);
    }
}

/// <summary>
/// Adapts a <see cref="ChannelReader{T}" /> to <see cref="IAsyncEnumerable{T}" />
/// while carrying queue-specific disposal semantics.
/// </summary>
internal sealed class AsyncSignalEnumerable<T>(
    ChannelReader<T> reader,
    bool respectConsumerCancellation,
    Func<ValueTask>? onDispose) : IAsyncEnumerable<T>
{
    /// <summary>
    /// Creates an enumerator that can either honor the caller's cancellation
    /// token or ignore it and keep draining the channel.
    /// </summary>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var token = respectConsumerCancellation ? cancellationToken : CancellationToken.None;
        return new AsyncSignalEnumerator<T>(reader, onDispose, token);
    }
}

/// <summary>
/// Reads raw channel items and turns channel termination into async-enumerator
/// semantics:
/// normal completion ends the sequence, faulted completion rethrows the
/// producer's original exception, and consumer cancellation remains distinct.
/// </summary>
internal sealed class AsyncSignalEnumerator<T>(
    ChannelReader<T> reader,
    Func<ValueTask>? onDispose,
    CancellationToken cancellationToken) : IAsyncEnumerator<T>
{
    private int _isDisposed;
    private bool _isTerminal;

    public T Current { get; private set; } = default!;

    /// <summary>
    /// Advances to the next buffered value, returns <see langword="false" />
    /// when the channel completes normally, or rethrows the producer failure
    /// once all buffered values have been drained.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync()
    {
        try
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!reader.TryRead(out var item)) { continue; }

                Current = item;
                return true;
            }
        }
        catch (Exception) when (ShouldSurfaceTerminalException()) { throw; }

        _isTerminal = true;
        return false;
    }

    /// <summary>
    /// Marks producer-driven completion as terminal while letting consumer
    /// cancellation flow through without changing the queue's terminal state.
    /// </summary>
    private bool ShouldSurfaceTerminalException()
    {
        if (cancellationToken.IsCancellationRequested) return false;

        _isTerminal = true;
        return true;
    }

    /// <summary>
    /// Notifies the owner only when enumeration stops before the producer has
    /// reached normal or faulted completion.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;

        // Only notify the owner when enumeration stops early. Once a terminal
        // signal has been observed, upstream cleanup has already happened.
        if (_isTerminal || onDispose is null) return;

        await onDispose().ConfigureAwait(false);
    }
}
