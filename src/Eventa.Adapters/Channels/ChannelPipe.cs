using System.Threading.Channels;

namespace Eventa.Adapters.Channels;

/// <summary>
/// Owns one connected in-memory pair of channel endpoints.
/// </summary>
public sealed class ChannelPipe : IDisposable
{
    private static readonly UnboundedChannelOptions ChannelOptions = new()
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false,
    };

    private int _disposed;

    /// <summary>
    /// Creates a connected pair of channel endpoints.
    /// </summary>
    /// <param name="options">Optional pair configuration.</param>
    public ChannelPipe(ChannelPipeOptions? options = null)
    {
        options ??= new ChannelPipeOptions();

        var leftToRight = Channel.CreateUnbounded<ChannelMessage>(ChannelOptions);
        var rightToLeft = Channel.CreateUnbounded<ChannelMessage>(ChannelOptions);
        var endpointOptions = new ChannelEndpointOptions
        {
            CompleteOutboundOnTerminal = true,
            ClosedEvent = options.ClosedEvent,
        };

        Left = new ChannelEndpoint(rightToLeft.Reader, leftToRight.Writer, endpointOptions);
        Right = new ChannelEndpoint(leftToRight.Reader, rightToLeft.Writer, endpointOptions);
    }

    /// <summary>
    /// Gets the left endpoint of the connected pair.
    /// </summary>
    public ChannelEndpoint Left { get; }

    /// <summary>
    /// Gets the right endpoint of the connected pair.
    /// </summary>
    public ChannelEndpoint Right { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        Left.Dispose();
        Right.Dispose();
    }
}
