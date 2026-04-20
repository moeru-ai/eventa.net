using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace Eventa;

/// <summary>
/// Bridges push-style producers to <see cref="IAsyncEnumerable{T}" /> while preserving
/// a single terminal transition.
/// </summary>
internal sealed class AsyncSignalQueue<T>
{
    private readonly Channel<AsyncSignal<T>> _channel = Channel.CreateUnbounded<AsyncSignal<T>>();
    private int _isTerminal;

    /// <summary>
    /// Writes a value if the queue has not already completed or faulted.
    /// </summary>
    public bool TryWrite(T value)
    {
        return Volatile.Read(ref _isTerminal) == 0
            && _channel.Writer.TryWrite(AsyncSignal<T>.FromValue(value));
    }

    /// <summary>
    /// Transitions the queue to its completed state. Subsequent calls are ignored.
    /// </summary>
    public void Complete()
    {
        if (Interlocked.Exchange(ref _isTerminal, 1) != 0)
        {
            return;
        }

        _channel.Writer.TryWrite(AsyncSignal<T>.Completed());
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Transitions the queue to its faulted state. Subsequent calls are ignored.
    /// </summary>
    public void Fault(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        if (Interlocked.Exchange(ref _isTerminal, 1) != 0)
        {
            return;
        }

        _channel.Writer.TryWrite(AsyncSignal<T>.FromError(error));
        _channel.Writer.TryComplete();
    }

    /// <summary>
    /// Exposes the queue as an async sequence. When enumeration stops early,
    /// <paramref name="onDispose" /> can be used to notify upstream owners.
    /// </summary>
    public IAsyncEnumerable<T> ReadAll(
        bool respectConsumerCancellation = true,
        Func<ValueTask>? onDispose = null)
    {
        return new AsyncSignalEnumerable<T>(_channel.Reader, respectConsumerCancellation, onDispose);
    }
}

/// <summary>
/// The three signal kinds carried through the internal channel.
/// </summary>
internal enum AsyncSignalKind
{
    Value,
    Error,
    Completed,
}

/// <summary>
/// Values and terminal signals share the same channel so the consumer sees
/// the exact order in which data, completion, and faults were produced.
/// </summary>
internal readonly record struct AsyncSignal<T>(T? Value, Exception? Error, AsyncSignalKind Kind)
{
    public static AsyncSignal<T> FromValue(T value)
    {
        return new AsyncSignal<T>(value, null, AsyncSignalKind.Value);
    }

    public static AsyncSignal<T> FromError(Exception error)
    {
        return new AsyncSignal<T>(default, error, AsyncSignalKind.Error);
    }

    public static AsyncSignal<T> Completed()
    {
        return new AsyncSignal<T>(default, null, AsyncSignalKind.Completed);
    }
}

internal sealed class AsyncSignalEnumerable<T>(
    ChannelReader<AsyncSignal<T>> reader,
    bool respectConsumerCancellation,
    Func<ValueTask>? onDispose) : IAsyncEnumerable<T>
{
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return new AsyncSignalEnumerator<T>(
            reader,
            respectConsumerCancellation ? cancellationToken : CancellationToken.None,
            onDispose);
    }
}

internal sealed class AsyncSignalEnumerator<T>(
    ChannelReader<AsyncSignal<T>> reader,
    CancellationToken cancellationToken,
    Func<ValueTask>? onDispose) : IAsyncEnumerator<T>
{
    private int _isDisposed;
    private bool _isTerminal;

    public T Current { get; private set; } = default!;

    public async ValueTask<bool> MoveNextAsync()
    {
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!reader.TryRead(out var signal))
            {
                continue;
            }

            switch (signal.Kind)
            {
                case AsyncSignalKind.Value:
                    Current = signal.Value!;
                    return true;
                case AsyncSignalKind.Error:
                    // Preserve the original stack when surfacing producer failures.
                    _isTerminal = true;
                    ExceptionDispatchInfo.Capture(signal.Error!).Throw();
                    break;
                case AsyncSignalKind.Completed:
                    _isTerminal = true;
                    return false;
            }
        }

        _isTerminal = true;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        // Only notify the owner when enumeration stops early. Once a terminal
        // signal has been observed, upstream cleanup has already happened.
        if (_isTerminal || onDispose is null)
        {
            return;
        }

        await onDispose().ConfigureAwait(false);
    }
}
