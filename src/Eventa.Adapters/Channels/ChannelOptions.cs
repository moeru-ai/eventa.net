using Eventa;

namespace Eventa.Adapters.Channels;

/// <summary>
/// Configures a connected in-memory channel pair.
/// </summary>
public sealed class ChannelPipeOptions
{
    /// <summary>
    /// Gets the public event emitted when either endpoint observes terminal closure.
    /// </summary>
    public EventDefinition<ChannelClosedPayload> ClosedEvent { get; init; } = ChannelEvents.Closed;
}

/// <summary>
/// Configures one custom channel endpoint.
/// </summary>
public sealed class ChannelEndpointOptions
{
    /// <summary>
    /// Gets whether endpoint disposal completes the supplied outbound writer.
    /// </summary>
    public bool CompleteOutboundOnDispose { get; init; }

    /// <summary>
    /// Gets the public event emitted when the endpoint observes terminal closure.
    /// </summary>
    public EventDefinition<ChannelClosedPayload> ClosedEvent { get; init; } = ChannelEvents.Closed;
}
